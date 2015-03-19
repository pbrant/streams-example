package code

import org.specs2.mutable.Specification

import org.joda.time._
import org.joda.time.DateTimeConstants._

import scalaz._
import Scalaz._

object StreamsExampleSpec {
  import StreamsExample._

  val startDateTime = new DateTime(2014, 6, 15, 12, 0)
  val weekStart = startDateTime.dayOfWeek.withMinimumValue.withTimeAtStartOfDay

  val recurring1 =
    List(
      RecurringEvent(MONDAY, new LocalTime(10, 0), MONDAY, new LocalTime(11, 0)),
      RecurringEvent(FRIDAY, new LocalTime(10, 0), MONDAY, new LocalTime(12, 0))
    )

  val recurring2 =
    List(
      RecurringEvent(MONDAY, new LocalTime(10, 0), MONDAY, new LocalTime(11, 0)),
      RecurringEvent(TUESDAY, new LocalTime(10, 0), TUESDAY, new LocalTime(11, 0)),
      RecurringEvent(TUESDAY, new LocalTime(11, 0), TUESDAY, new LocalTime(12, 0))
    )

  val oneTime1 = List(
    OneTimeEvent(mkDateTime(MONDAY, (10, 0)), mkDateTime(MONDAY, (14, 30)), true),
    OneTimeEvent(mkDateTime(MONDAY, (10, 15)), mkDateTime(MONDAY, (10, 35)), false),
    OneTimeEvent(mkDateTime(MONDAY, (10, 45)), mkDateTime(MONDAY, (10, 55)), false),
    OneTimeEvent(mkDateTime(MONDAY, (10, 15)), mkDateTime(MONDAY, (10, 30)), false)
  )

  val oneTime2 = List(
    OneTimeEvent(mkDateTime(MONDAY, (8, 0)), mkDateTime(WEDNESDAY, (12, 0)), false),
    OneTimeEvent(mkDateTime(FRIDAY, (12, 0)), mkDateTime(WEDNESDAY, (12, 0)).plusWeeks(1), false)
  )

  val oneTime3 = List(
    OneTimeEvent(mkDateTime(MONDAY, (10, 0)).plusWeeks(-1), mkDateTime(MONDAY, (9, 0)), false),
    OneTimeEvent(mkDateTime(MONDAY, (10, 0)), mkDateTime(MONDAY, (13, 0)), false)
  )

  def stream1 = recurringEvents(startDateTime, recurring1)
  def stream2 = recurringEvents(startDateTime, recurring2)

  def mkDateTime(dayOfWeek: Int, time: (Int, Int)): DateTime =
    weekStart.withDayOfWeek(dayOfWeek).withFields(new LocalTime(time._1, time._2))

  def mkTimeInterval(start: (Int, Int), end: (Int, Int)) = mkInterval(MONDAY, start, end)

  def mkInterval(dayOfWeek: Int, start: (Int, Int), end: (Int, Int)) =
    new Interval(
      weekStart.withDayOfWeek(dayOfWeek).withFields(new LocalTime(start._1, start._2)),
      weekStart.withDayOfWeek(dayOfWeek).withFields(new LocalTime(end._1, end._2))
    )
}

class StreamsExampleSpec extends Specification {
  import StreamsExample._
  import StreamsExampleSpec._

  "Recurring events" >> {
    "should repeat weekly" >> {
      val l = stream1.take(3).toList

      l(0).getStart must_== l(2).getStart.plusWeeks(-1)
    }
  }

  "Two interval streams" >> {
    "can be ordered" >> {
      val result = ordered(stream1, stream2)

      val r1 = stream1.take(2).toList
      val r2 = stream2.take(3).toList

      val all = result.take(5).toList

      all(0) must_== r1(0)
      all(1) must_== r2(0)
      all(2) must_== r2(1)
      all(3) must_== r2(2)
      all(4) must_== r1(1)
    }
  }

  "Subtracting one interval from another" >> {
    "should return interval to keep if discard interval does not overlap" >> {
      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((9, 0), (10, 0))
      ) must_== List(mkTimeInterval((10, 0), (11, 0)))
    }

    "should return nothing if interval to discard entirely covers the one to keep" >> {
      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((9, 0), (12, 0))
      ) must_== Nil

      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((10, 0), (11, 0))
      ) must_== Nil
    }

    "should return two intervals if interval to discard is in the middle" >> {
      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((10, 30), (10, 45))
      ) must_== List(mkTimeInterval((10, 0), (10, 30)), mkTimeInterval((10, 45), (11, 0)))
    }

    "should keep end if discard interval covers first part" >> {
      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((10, 0), (10, 45))
      )(0) must_== mkTimeInterval((10, 45), (11, 0))

      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((9, 0), (10, 45))
      )(0) must_== mkTimeInterval((10, 45), (11, 0))
    }

    "should keep start if discard interval covers last part" >> {
      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((10, 30), (11, 0))
      )(0) must_== mkTimeInterval((10, 0), (10, 30))

      subtract(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((10, 30), (11, 15))
      )(0) must_== mkTimeInterval((10, 0), (10, 30))
    }
  }

  "removeNotAllowed" >> {
    "must remove excluded intervals" >> {
      val notAllowed = List(
        mkInterval(MONDAY, (9, 0), (10, 15)),
        mkInterval(MONDAY, (10, 30), (10, 45))
      )
      val result = removeNotAllowed(stream1, notAllowed).take(2).toList
      result must_== List(mkInterval(MONDAY, (10, 15), (10, 30)), mkInterval(MONDAY, (10, 45), (11, 0)))
    }
  }

  "Interval merging" >> {
    def doMerge(a: (Int, Int), b: (Int, Int), c: (Int, Int), d: (Int, Int)): Option[Interval] =
      combine(
        mkTimeInterval(a, b),
        mkTimeInterval(c, d)
      )

    "should merge intervals that abut" >> {
      doMerge((10, 0), (11, 0), (11, 0), (21, 0)) must_== mkTimeInterval((10, 0), (21, 0)).some
      doMerge((11, 0), (21, 0), (10, 0), (11, 0)) must_== mkTimeInterval((10, 0), (21, 0)).some
    }

    "should merge intervals that overlap" >> {
      doMerge((10, 0), (11, 0), (10, 30), (11, 0)) must_== mkTimeInterval((10, 0), (11, 0)).some
      doMerge((10, 0), (11, 0), (10, 15), (10, 30)) must_== mkTimeInterval((10, 0), (11, 0)).some
      doMerge((10, 0), (11, 0), (10, 30), (11, 30)) must_== mkTimeInterval((10, 0), (11, 30)).some
    }

    "should not merge intervals separated by a gap" >> {
      doMerge((10, 0), (11, 0), (12, 0), (13, 0)) must_== None
    }
  }

  "Merging adjacent intervals" >> {
    "should merge (example #1)" >> {
      val s = List(
        mkTimeInterval((10, 0), (11, 0)),
        mkTimeInterval((11, 0), (12, 0))
      ).toEphemeralStream

      merge(s).toList must_== List(mkTimeInterval((10, 0), (12, 0)))
    }

    "should merge (example #2)" >> {
      val expected = List(
        mkInterval(MONDAY, (10, 0), (11, 0)),
        mkInterval(TUESDAY, (10, 0), (12, 0))
      )
      merge(ordered(stream1, stream2)).take(2).toList must_== expected
    }

    def recurringAndOneTime(l: List[OneTimeEvent]): IntervalStream = {
      val streams = List(
        stream1,
        stream2,
        allowed(l)
      )
      val all = streams.foldLeft(EphemeralStream[Interval])(ordered(_, _))
      removeNotAllowed(merge(all), notAllowed(l))
    }

    "should merge (example #3)" >> {
      val expected = List(
        mkInterval(MONDAY, (10, 0), (10, 15)),
        mkInterval(MONDAY, (10, 35), (10, 45)),
        mkInterval(MONDAY, (10, 55), (14, 30))
      )

      recurringAndOneTime(oneTime1).take(3).toList must_== expected
    }

    "should merge (example #4)" >> {
      val expected = List(
        mkInterval(FRIDAY, (10, 0), (12, 0))
      )
      recurringAndOneTime(oneTime2).take(1).toList must_== expected
    }
  }

  "Complete interval stream" >> {
    "should consider recurring events that wrap" >> {
      val expected = List(
        mkInterval(MONDAY, (9, 0), (10, 0))
      )
      val s = intervals(mkDateTime(MONDAY, (8, 0)), recurring1 ++ recurring2, oneTime3)

      s.take(1).toList must_== expected
    }
  }
}
