package code

import org.joda.time._

import scalaz.{Ordering => _, _}
import Scalaz._
import scalaz.\&/.{Both, That, This}

import scala.language.higherKinds

case class RecurringEvent(
  startDay: Int,
  startTime: LocalTime,
  endDay: Int,
  endTime: LocalTime
) {
  def toInterval(weekStart: DateTime): Interval = {
    val end = weekStart.withDayOfWeek(endDay).withFields(endTime)
    new Interval(
      weekStart.withDayOfWeek(startDay).withFields(startTime),
      if (endDay < startDay)
        end.plusWeeks(1)
      else
        end
    )
  }
}

object RecurringEvent {
  implicit val ordering = math.Ordering.by[RecurringEvent, (Int, ReadablePartial)] { e =>
    (e.startDay, e.startTime)
  }
}

case class OneTimeEvent(
  startDate: DateTime,
  endDate: DateTime,
  isAllowed: Boolean
) {
  def toInterval: Interval = new Interval(startDate, endDate)
}

object OneTimeEvent {
  implicit val ordering = math.Ordering.by[OneTimeEvent, ReadableInstant](_.startDate)
}

object StreamsExample {
  type IntervalStream = EphemeralStream[Interval]

  import EphemeralStream._

  private case class EventState(weekStart: DateTime, index: Int, events: IndexedSeq[RecurringEvent]) {
    def current: Interval = events(index).toInterval(weekStart)

    def nextState: EventState = {
      if (index == events.length - 1)
        EventState(weekStart.plusWeeks(1), 0, events)
      else
        copy(index = index + 1)
    }
  }

  /** Returns an infinite stream of recurring events starting from `start` */
  def recurringEvents(start: DateTime, events: List[RecurringEvent]): IntervalStream = {
    if (events.isEmpty)
      EphemeralStream[Interval]
    else {
      val weekStart = start.dayOfWeek.withMinimumValue.withTimeAtStartOfDay
      val initial = EventState(weekStart, 0, events.sorted.toIndexedSeq)

      unfold(initial)(s => (s.current, s.nextState).some)
    }
  }

  /** Returns a stream of allowed one-time events */
  def allowed(events: List[OneTimeEvent]): IntervalStream =
    events.sorted.filter(_.isAllowed).map(_.toInterval).toEphemeralStream

  /** Returns a list of not allowed one-time events */
  def notAllowed(events: List[OneTimeEvent]): List[Interval] =
    events.sorted.filterNot(_.isAllowed).map(_.toInterval)

  private def flatten[A](o: Option[EphemeralStream[A]]): EphemeralStream[A] =
    o.getOrElse(EphemeralStream[A])

  /** Interleaves two ordered interval streams as another ordered interval stream */
  def ordered(s1: IntervalStream, s2: IntervalStream): IntervalStream = {
    (s1.headOption, s2.headOption) match {
      case (Some(i1), Some(i2)) => {
        if (i2.getStart.isBefore(i1.getStart))
          cons(i2, ordered(s1, flatten(s2.tailOption)))
        else
          cons(i1, ordered(flatten(s1.tailOption), s2))
      }
      case (_, None) => s1
      case (None, Some(_)) => s2
    }
  }

  /** Returns the portions of `keep` not covered by `discard` */
  def subtract(keep: Interval, discard: Interval): List[Interval] = {
    val o = discard.overlap(keep)
    if (o eq null)
      List(keep)
    else {
      if (o == keep)
        Nil
      else if (discard.getStart.isBefore(keep.getStart) || discard.getStart == keep.getStart)
        List(new Interval(discard.getEnd, keep.getEnd))
      else if (o == discard)
        List(
          new Interval(keep.getStart, discard.getStart),
          new Interval(discard.getEnd, keep.getEnd)
        )
      else
        List(new Interval(keep.getStart, discard.getStart))
    }
  }

  /** Returns an interval stream with the intervals in `notAllowed` removed */
  def removeNotAllowed(s1: IntervalStream, notAllowed: List[Interval]): IntervalStream = {
    s1.flatMap { i =>
      notAllowed.foldLeft(List(i)) { (l, n) =>
        l.flatMap(subtract(_, n))
      }.toEphemeralStream
    }
  }

  private def minDateTime(a: DateTime, b: DateTime): DateTime =
    if (b.isBefore(a))
      b
    else
      a

  private def maxDateTime(a: DateTime, b: DateTime): DateTime =
    if (a.isAfter(b))
      a
    else
      b

  /** Returns a combined interval if `a` and `b` abut or overlap */
  def combine(a: Interval, b: Interval): Option[Interval] = {
    if (a.abuts(b) || a.overlaps(b))
      new Interval(
        minDateTime(a.getStart, b.getStart),
        maxDateTime(a.getEnd, b.getEnd)
      ).some
    else
      none
  }

  /** Returns an interval stream with abutting or overlapping neighbors merged together */
  def merge(intervals: IntervalStream): IntervalStream = {
    intervals match {
      case h1 ##:: h2 ##:: t => {
        combine(h1, h2)
          .map(i => merge(cons(i, t)))
          .getOrElse(cons(h1, merge(cons(h2, t))))
      }
      case s => s
    }
  }

  /** Returns an interval stream starting from `startDate` using `recurring`
    * and `oneTime` as event sources (or exclusions).
    */
  def intervals(
    startDate: DateTime,
    recurring: List[RecurringEvent],
    oneTime: List[OneTimeEvent]
  ): IntervalStream = {
    removeNotAllowed(
      merge(
        ordered(
          recurringEvents(startDate.plusWeeks(-1), recurring),
          allowed(oneTime))),
      notAllowed(oneTime)
    ).dropWhile(i => i.getEnd.isBefore(startDate) || i.getEnd == startDate)
  }
}
