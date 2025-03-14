mod tests {

    use sqlx_duckdb::interval::Interval;
    use std::time::Duration;

    #[test]
    fn interval_1() {
        let interval = Interval::default();
        let duration = Duration::default();

        assert!(interval.is_zero());

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_2() {
        let interval = Interval::default();
        let duration = Duration::from_nanos(500);

        // Interval has microseconds precision therefore the 500nanos are lost
        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_ne!(duration, interval.as_duration(30.0));
        assert_ne!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_3() {
        let interval = Interval::default();
        let duration = Duration::from_nanos(999);

        // Interval has microseconds precision therefore the 999nanos are lost
        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_ne!(duration, interval.as_duration(30.0));
        assert_ne!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_4() {
        let interval = Interval::from_micros(1);
        let duration = Duration::from_nanos(1_000);

        assert!(!interval.is_zero());

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_5() {
        let interval = Interval::from_micros(2);
        let duration = Duration::from_nanos(2_000);

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_6() {
        let interval = Interval::from_micros(2);
        let duration = Duration::from_nanos(2_001);

        // Interval has micros precision therefore the extra 1 nanos is discarded
        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_ne!(duration, interval.as_duration(30.0));
        assert_ne!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_7() {
        let interval = Interval::from_micros(2_001);
        let duration = Duration::from_millis(2);

        assert_ne!(interval.as_duration(30.0), duration);
        assert_ne!(duration, interval.as_duration(30.0));

        assert_ne!(interval, Interval::from_duration(&duration));
        assert_ne!(Interval::from_duration(&duration), interval);
    }

    #[test]
    fn interval_8() {
        const DAYS: u64 = 365 * 10000 + 365 * 100;
        let interval = Interval::from_secs(DAYS * 60 * 60 * 24);
        let duration = Duration::from_secs(DAYS * 60 * 60 * 24);

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_9() {
        const DAYS: u64 = 365 * 5_030_636;
        let interval = Interval::from_mins(DAYS * 60 * 24);
        let duration = Duration::from_secs(DAYS * 60 * 60 * 24);

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_10() {
        let micros = 10u64.pow(15);
        let interval = Interval::from_micros(micros);
        let duration = Duration::from_micros(micros);

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_11() {
        let days = i32::MAX;
        let interval = Interval::from_days(days as u64);
        let duration = Duration::from_secs(days as u64 * 86400);

        assert_eq!(interval, Interval::from_duration(&duration));
        assert_eq!(Interval::from_duration(&duration), interval);

        assert_eq!(duration, interval.as_duration(30.0));
        assert_eq!(interval.as_duration(30.0), duration);
    }

    #[test]
    fn interval_12() {
        let weeks = 350;
        let interval = Interval::from_weeks(weeks);
        let expected_duration = Duration::from_secs(weeks * 7 * 86400);

        assert_eq!(interval.as_duration(30.0), expected_duration);
    }
}
