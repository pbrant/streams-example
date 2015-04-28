package code

import org.joda.time._

import scalaz.{Ordering => _, _}
import Scalaz._
import scalaz.\&/.{Both, That, This}

import scala.language.higherKinds

object Util {
  implicit val localTimeOrdering = new Ordering[LocalTime] {
    def compare(a: LocalTime, b: LocalTime): Int = a.compareTo(b)
  }

  def combine[F[_],A](a1: F[A], a2: F[A])(f: (A, A) => A)(implicit F: Align[F]): F[A] = {
    F.alignWith[A,A,A](_ match {
      case This(a) => a
      case That(a) => a
      case Both(a1, a2) => f(a1, a2)
    })(a1, a2)
  }
}

case class WorkDay(
  start: LocalTime,
  end: LocalTime,
  lunch: Option[(LocalTime, LocalTime)]
) {
  def merge(other: WorkDay)(implicit O: Ordering[LocalTime]): WorkDay = {
    WorkDay(
      O.max(this.start, other.start),
      O.min(this.end, other.end),
      Util.combine(this.lunch, other.lunch) { case ((s1, e1), (s2, e2)) =>
        (O.min(s1, s2), O.max(e1, e2))
      }
    )
  }

  def interval(current: LocalDate, withLunch: Boolean, morning: Boolean): Interval =
    this.lunch
      .filter(_ => withLunch)
      .map { case (lunchStart, lunchEnd) =>
        if (morning)
          new Interval(current.toDateTime(this.start), current.toDateTime(lunchStart))
        else
          new Interval(current.toDateTime(lunchEnd), current.toDateTime(this.end))
      }
      .getOrElse(
        new Interval(current.toDateTime(this.start), current.toDateTime(this.end))
      )

  private def shorterOrEquals_?(a: Period, b: Period): Boolean = {
    val ad = a.toStandardDuration
    val bd = b.toStandardDuration
    ad.isShorterThan(bd) || ad.equals(bd)
  }

  def canUseLunch_?(period: Period): Boolean = {
    lunch.map { case (s, e) =>
      shorterOrEquals_?(period, new Period(start, s)) &&
        shorterOrEquals_?(period, new Period(e, end))
    }.getOrElse(false)
  }

  def multiDaySearch_?(period: Period): Boolean =
    ! shorterOrEquals_?(period, new Period(start, end))
}

object StreamsExample2 {
  type IntervalStream = EphemeralStream[Interval]

  import Free._
  import EphemeralStream._

  private def tail[A](s: EphemeralStream[A]): EphemeralStream[A] =
    s.tailOption.getOrElse(EphemeralStream[A])

  private case class WorkDayStreamState(current: LocalDate, morning: Boolean) {
    def nextState(workDay: WorkDay, withLunch: Boolean): WorkDayStreamState = {
      if (workDay.lunch.isDefined && withLunch && morning)
        this.copy(morning = false)
      else
        WorkDayStreamState(nextDate, true)
    }

    private def nextDate: LocalDate =
      if (current.getDayOfWeek == DateTimeConstants.FRIDAY)
        current.plusDays(3)
      else
        current.plusDays(1)
  }

  def workDays(start: LocalDate, workDay: WorkDay, withLunch: Boolean): IntervalStream = {
    val startOfWeek = start.dayOfWeek.withMinimumValue
    unfold(WorkDayStreamState(startOfWeek, true))(s =>
      (workDay.interval(s.current, withLunch, s.morning), s.nextState(workDay, withLunch)).some
    )
  }

  def overlapT(s1: IntervalStream, s2: IntervalStream): Trampoline[IntervalStream] = {
    def remaining(i1: Interval, i2: Interval, o: Interval) = {
      if (i1.getEnd == o.getEnd)
        overlapT(tail(s1), cons(new Interval(o.getEnd, i2.getEnd), tail(s2)))
      else
        overlapT(cons(new Interval(o.getEnd, i1.getEnd), tail(s1)), tail(s2))
    }

    (s1.headOption, s2.headOption) match {
      case (Some(i1), Some(i2)) => {
        if (i1 == i2)
          return_(cons(i1, overlap(tail(s1), tail(s2))))
        else if (i1.isBefore(i2)) {
          suspend(overlapT(tail(s1), s2))
        } else if (i1.isAfter(i2))
          suspend(overlapT(s1, tail(s2)))
        else {
          val o = i1.overlap(i2)
          return_(cons(o, remaining(i1, i2, o).run))
        }
      }
      case _ => return_(EphemeralStream[Interval])
    }
  }

  def overlap(s1: IntervalStream, s2: IntervalStream): IntervalStream =
    overlapT(s1, s2).run

  def subdivide(intervals: IntervalStream, period: Period): IntervalStream = {
    intervals.flatMap { interval =>
      unfold(interval) { rest =>
        if (period.toStandardDuration.isLongerThan(rest.toDuration))
          none
        else
          (rest.withPeriodAfterStart(period), rest.withStart(rest.getStart.plus(period))).some
      }
    }
  }

  def takeWhileAdjacent(
    intervals: IntervalStream,
    workDay: WorkDay,
    holidays: Set[LocalDate]
  ): IntervalStream =
    takeWhilePairs(intervals)(adjacent(workDay, holidays, _, _))

  def mergeAbutting(s: IntervalStream): IntervalStream =
    mergePairs(s)((a1, a2) => option(a1.abuts(a2), new Interval(a1.getStart, a2.getEnd)))

  def takeWhilePairs[A](s: EphemeralStream[A])(op: (A, A) => Boolean): EphemeralStream[A] = {
    s match {
      case h1 ##:: h2 ##:: t =>
        if (op(h1, h2))
          cons(h1, takeWhilePairs(cons(h2, t))(op))
        else
          EphemeralStream(h1)
      case s => s
    }
  }

  def mergePairs[A](s: EphemeralStream[A])(op: (A, A) => Option[A]): EphemeralStream[A] = {
    s match {
      case h1 ##:: h2 ##:: t => {
        op(h1, h2) match {
          case Some(a) => mergePairs(cons(a, t))(op)
          case _ => cons(h1, mergePairs(cons(h2, t))(op))
        }
      }
      case s => s
    }
  }

  def scanLeft[A, B](s: EphemeralStream[A])(z: B)(op: (B, A) => B): EphemeralStream[B] = {
    s match {
      case h ##:: t => cons(z, scanLeft(t)(op(z, h))(op))
      case _ => EphemeralStream(z)
    }
  }

  def durationPartialSums(s: IntervalStream): EphemeralStream[Duration] =
    scanLeft(s)(Duration.ZERO)((d, i) => d.plus(i.toDuration))

  def fitToTargetDuration(
    target: Duration,
    s: EphemeralStream[(Duration, Interval)]
  ): EphemeralStream[(Duration, Interval)] = {
    s.reverse match {
      case (d, i) ##:: t => {
        if (! d.plus(i.toDuration).isShorterThan(target)) {
          val tooMuch = d.plus(i.toDuration).minus(target)
          cons((d, new Interval(i.getStart, i.getEnd.minus(tooMuch))), t).reverse
        } else {
          EphemeralStream[(Duration, Interval)]()
        }
      }
      case _ => s
    }
  }

  def multiDayIntervals(
    intervals: IntervalStream,
    workDay: WorkDay,
    holidays: Set[LocalDate],
    period: Period
  ): EphemeralStream[IntervalStream] = {
    val targetDuration = fitToWorkDay(period, workDay).toStandardDuration
    intervals
      .tails
      .filter(_.headOption.exists(_.getStart.toLocalTime == workDay.start))
      .map(takeWhileAdjacent(_, workDay, holidays))
      .map(is => durationPartialSums(is) zip is)
      .map(_.takeWhile { case (d, i) => d.isShorterThan(targetDuration) })
      .map(fitToTargetDuration(targetDuration, _))
      .map(_.map(_._2))
      .filter(! _.isEmpty)
  }

  def freeIntervals(
    freeTimes: IntervalStream,
    holidays: Set[LocalDate],
    startSearch: LocalDate,
    workDay: WorkDay,
    period: Period
  ): EphemeralStream[IntervalStream] = {
    val workDayPeriod = fitToWorkDay(period, workDay)

    val allWorkDays = workDays(
      startSearch,
      workDay,
      workDay.canUseLunch_?(workDayPeriod)
    )

    val input = mergeAbutting(overlap(freeTimes, allWorkDays))

    if (! workDay.multiDaySearch_?(workDayPeriod))
      subdivide(input, workDayPeriod).map(EphemeralStream(_))
    else
      multiDayIntervals(input, workDay, holidays, workDayPeriod)
  }

  def fitToWorkDay(period: Period, workDay: WorkDay): Period = {
    val workDayPeriod = new Period(workDay.start, workDay.end)
    period
      .minusDays(period.getDays)
      .plus(workDayPeriod.multipliedBy(period.getDays))
      .minusWeeks(period.getWeeks)
      .plus(workDayPeriod.multipliedBy(period.getWeeks*5))
  }

  def adjacent(workDay: WorkDay,
    holidays: Set[LocalDate],
    i1: Interval,
    i2: Interval
  ): Boolean = {
    def adjacentToNextWorkDay_?(_i1: Interval, _i2: Interval): Boolean = {
      if (_i2.isAfter(_i1) &&
        _i1.getEnd.toLocalTime == workDay.end &&
        _i2.getStart.toLocalTime == workDay.start) {
        val days = Days.daysBetween(_i1.getEnd.toLocalDate, _i2.getStart.toLocalDate).getDays
        if (days == 1) {
          true
        } else {
          (1 until days)
            .map(_i1.getEnd.toLocalDate.plusDays)
            .forall(d =>
              d.getDayOfWeek == DateTimeConstants.SATURDAY ||
              d.getDayOfWeek == DateTimeConstants.SUNDAY ||
              holidays.contains(d)
            )
        }
      } else {
        false
      }
    }

    i1.abuts(i2) || adjacentToNextWorkDay_?(i1, i2)
  }
}
