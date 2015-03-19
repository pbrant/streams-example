package code

import org.specs2.mutable.Specification

import org.joda.time._
import org.joda.time.DateTimeConstants._

import scalaz._
import Scalaz._

class StreamsExample2Spec extends Specification {
  import StreamsExample2._

  def mkTime(ms: Int, second: Int) = new LocalTime(10, 0, second, ms).toDateTimeToday
  def mkI(startMs: Int, endMs: Int, second: Int = 0) = new Interval(mkTime(startMs, second), mkTime(endMs, second))

  def mkIntervalStream(r: Range, second: Int = 0): EphemeralStream[Interval] =
    EphemeralStream(r.toList.map(s => mkI(s, s+1, second)): _*)

  def mkPeriod(ms: Int): Period = new Period(0, 0, 0, ms)

  def mkBigStream(startSecond: Int, endSecond: Int): IntervalStream = {
    (for {
      s <- startSecond until endSecond
    } yield mkIntervalStream(0 until 999, s)).reduce(_ ++ _)
  }

  def stream(is: Interval*) = EphemeralStream[Interval](is: _*)

  "Empty interval streams" >> {
    "should not contain overlapping intervals (left side)" >> {
      overlap(EphemeralStream[Interval], mkIntervalStream(0 to 3)).toList must_== Nil
    }

    "should not contain overlapping intervals (right side)" >> {
      overlap(mkIntervalStream(0 to 3), EphemeralStream[Interval]).toList must_== Nil
    }
  }

  "Identical interval streams" >> {
    "should always overlap" >> {
      val s = mkIntervalStream(1 to 50)
      overlap(s, s).toList must_== s.toList
    }
  }

  "Non-overlapping interval streams" >> {
    "should never overlap" >> {
      val s1 = mkIntervalStream(1 until 50) ++ mkIntervalStream(150 until 200)
      val s2 = mkIntervalStream(50 until 150)
      overlap(s1, s2).toList must_== Nil
    }
  }

  "Overlapping interval streams" >> {
    "s(i(0, 5)) overlap s(i(3, 10)) = s(i(3, 5))" >> {
      overlap(stream(mkI(0, 5)), stream(mkI(3, 10))).toList must_== stream(mkI(3, 5)).toList
    }

    "s(i(3, 10)) overlap s(i(0, 5)) = s(i(3, 5))" >> {
      overlap(stream(mkI(3, 10)), stream(mkI(0, 5))).toList must_== stream(mkI(3, 5)).toList
    }

    "s(i(0, 10)) overlap s(i(0, 5)) = s(i(0, 5))" >> {
      overlap(stream(mkI(0, 10)), stream(mkI(0, 5))).toList must_== stream(mkI(0, 5)).toList
    }

    "s(i(0, 5)) overlap s(i(0, 10)) = s(i(0, 5))" >> {
      overlap(stream(mkI(0, 5)), stream(mkI(0, 10))).toList must_== stream(mkI(0, 5)).toList
    }

    "s(i(0, 20), i(25, 35)) overlap s(i(5, 10), i(15, 30)) = s(i(5, 10), i(25, 30))" >> {
      overlap(stream(mkI(0, 20), mkI(25, 35)), stream(mkI(5, 10), mkI(15, 30))).toList must_==
        stream(mkI(5, 10), mkI(15, 20), mkI(25, 30)).toList
    }

    "s(i(5, 10), i(15, 30)) overlap s(i(0, 20), i(25, 35)) = s(i(5, 10), i(25, 30))" >> {
      overlap(stream(mkI(5, 10), mkI(15, 30)), stream(mkI(0, 20), mkI(25, 35))).toList must_==
        stream(mkI(5, 10), mkI(15, 20), mkI(25, 30)).toList
    }
  }

  "Large interval streams" >> {
    "should not overflow the stack" >> {
      val s1 = mkBigStream(0, 30)
      val s2 = mkBigStream(31,55)

      overlap(s1, s2).toList must_== Nil
    }
  }

  "Subdividing interval streams" >> {
    "should remove intervals that are too small" >> {
      subdivide(stream(mkI(0, 5), mkI(10, 15), mkI(18, 23)), mkPeriod(10)).toList must_== Nil
    }

    "should divide intervals into period sized intervals (test #1)" >> {
      val s = stream(mkI(0, 5), mkI(10, 15), mkI(18, 23))
      subdivide(s, mkPeriod(5)).toList must_== s.toList
    }

    "should divide intervals into period sized intervals (test #2)" >> {
      val input = stream(mkI(0, 12), mkI(14, 18), mkI(20, 25))
      val output = stream(mkI(0, 5), mkI(5, 10), mkI(20, 25))
      subdivide(input, mkPeriod(5)).toList must_== output.toList
    }
  }

  val start = new LocalDate(2015, 4, 18)
  val monday = new LocalDate(2015, 4, 13)
  val tuesday = new LocalDate(2015, 4, 14)
  val wednesday = new LocalDate(2015, 4, 15)
  val friday = new LocalDate(2015, 4, 17)
  val nextMonday = new LocalDate(2015, 4, 20)

  val workDay = WorkDay(new LocalTime(8, 0, 0), new LocalTime(17, 0, 0), None)
  val workDayWithLunch = WorkDay(
    new LocalTime(8, 0, 0),
    new LocalTime(17, 0, 0),
    (new LocalTime(11, 0, 0), new LocalTime(12, 0, 0)).some
  )

  "A work-day stream" >> {
    "should not be affected by withLunch if no lunch available" >> {
      workDays(start, workDay, true).take(15).toList must_== workDays(start, workDay, false).take(15).toList
    }

    "must skip weekends (no lunch)" >> {
      workDays(start, workDay, false).take(15).toList(14).getStart.toLocalDate must_== new LocalDate(2015, 5, 1)
      workDays(start, workDayWithLunch, false).take(15).toList(14).getStart.toLocalDate must_== new LocalDate(2015, 5, 1)
    }

    "must skip weekends (with lunch)" >> {
      workDays(start, workDay, true).take(15).toList(14).getStart.toLocalDate must_== new LocalDate(2015, 5, 1)
      workDays(start, workDayWithLunch, true).take(30).toList(29).getStart.toLocalDate must_== new LocalDate(2015, 5, 1)
    }

    "must generate correct intervals (work-day w/o lunch)" >> {
      val i = workDays(start, workDay, false).take(1).toList(0)
      i must_== new Interval(monday.toDateTime(workDay.start), monday.toDateTime(workDay.end))
    }

    "must generate correct intervals (work-day with lunch)" >> {
      val l = workDays(start, workDayWithLunch, true).take(2).toList
      l(0) must_== new Interval(monday.toDateTime(workDay.start), monday.toDateTime(new LocalTime(11, 0, 0)))

      l(1) must_== new Interval(monday.toDateTime(new LocalTime(12, 0, 0)), monday.toDateTime(workDay.end))
    }
  }

  "Adjacent intervals" >> {
    "can directly abut" >> {
      adjacent(workDay, Set[LocalDate](), mkI(0, 10), mkI(10, 20)) must_== true
    }

    "can end one work-day and start another" >> {
      val i1 = new Interval(monday.toDateTime(new LocalTime(12, 0, 0)), monday.toDateTime(workDay.end))
      val i2 = new Interval(tuesday.toDateTime(workDay.start), tuesday.toDateTime(workDay.end))

      adjacent(workDay, Set[LocalDate](), i1, i2) must_== true
    }

    "can be separated by a week-end" >> {
      val i1 = new Interval(friday.toDateTime(new LocalTime(12, 0, 0)), friday.toDateTime(workDay.end))
      val i2 = new Interval(nextMonday.toDateTime(workDay.start), nextMonday.toDateTime(workDay.end))

      adjacent(workDay, Set[LocalDate](), i1, i2) must_== true
    }

    "can be separated by a holiday" >> {
      val i1 = new Interval(monday.toDateTime(new LocalTime(12, 0, 0)), monday.toDateTime(workDay.end))
      val i2 = new Interval(wednesday.toDateTime(workDay.start), wednesday.toDateTime(workDay.end))

      adjacent(workDay, Set[LocalDate](tuesday), i1, i2) must_== true
    }
  }

  "takeWhileAdjacent" >> {
    "should return an empty stream when provided with the same" >> {
      takeWhileAdjacent(EphemeralStream[Interval](), workDay, Set[LocalDate]()).toList must_== Nil
    }

    "should return that element when provided with a stream of one element" >> {
      val s = stream(mkI(0, 5))
      takeWhileAdjacent(s, workDay, Set[LocalDate]()).toList must_== s.toList
    }

    "should stop at the first non-adjacent neighbor (test #1)" >> {
      val s = stream(mkI(0, 5), mkI(5, 10))
      takeWhileAdjacent(s, workDay, Set[LocalDate]()).toList must_== s.toList
    }

    "should stop at the first non-adjacent neighbor (test #2)" >> {
      val in = stream(mkI(0, 5), mkI(5, 10), mkI(15, 20))
      val expected = stream(mkI(0, 5), mkI(5, 10))
      takeWhileAdjacent(in, workDay, Set[LocalDate]()).toList must_== expected.toList
    }

    "should stop at the first non-adjacent neighbor (test #3)" >> {
      val in = stream(mkI(0, 5), mkI(6, 10), mkI(15, 20))
      val expected = stream(mkI(0, 5))
      takeWhileAdjacent(in, workDay, Set[LocalDate]()).toList must_== expected.toList
    }
  }

  "durationPartialSums" >> {
    "should calculate durations correctly" >> {
      durationPartialSums(stream(mkI(5, 10), mkI(15, 22), mkI(40, 50))).map(_.getMillis).toList must_== List(0, 5, 12, 22)
    }
  }

  "multiDayIntervals" >> {
    val allWorkDays = workDays(monday, workDay, false)

    val oneAndAHalfDays = Period.days(1).plusHours(4)

    val mondayNoonToFriday = new Interval(
      monday.toDateTime(workDay.start.plus(Period.hours(4))),
      friday.toDateTime(new LocalTime(23, 0, 0))
    )

    val mondayToFriday = overlap(allWorkDays, EphemeralStream[Interval](mondayNoonToFriday))

    def oneAndAHalfWorkdays(start: LocalDate): List[Interval] =
      List(
        new Interval(start.toDateTime(workDay.start), start.toDateTime(workDay.end)),
        new Interval(start.plusDays(1).toDateTime(workDay.start), start.plusDays(1).toDateTime(new LocalTime(12, 0, 0)))
      )

    def headAsList(s: EphemeralStream[IntervalStream]): List[Interval] =
      s.headOption.map(_.toList).toList.flatten

    def tail[A](s: EphemeralStream[A]): EphemeralStream[A] =
      s.tailOption.getOrElse(EphemeralStream[A])

    "should shorten last interval" >> {
      val l = headAsList(multiDayIntervals(allWorkDays, workDay, Set[LocalDate](), oneAndAHalfDays))

      l must_== oneAndAHalfWorkdays(monday)
    }

    "should always start at beginning of day" >> {
      val l = headAsList(multiDayIntervals(mondayToFriday, workDay, Set[LocalDate](), oneAndAHalfDays))

      l must_== oneAndAHalfWorkdays(tuesday)
    }

    "should return all results" >> {
      val s = multiDayIntervals(mondayToFriday, workDay, Set[LocalDate](), oneAndAHalfDays)

      headAsList(s) must_== oneAndAHalfWorkdays(tuesday)
      headAsList(tail(s)) must_== oneAndAHalfWorkdays(wednesday)
    }
  }
}
