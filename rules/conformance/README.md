# Rule conformance vectors

A rule is evaluated in two places: in Flink on live traffic, and in the rules service when an analyst
backtests a draft against the lakehouse. If the two disagreed, a backtest would promise one alert
volume and production would deliver another. Each file here is a rule set, a sequence of payments in
arrival order and the alerts that must come out. Both engines run every file in CI:

* Flink: `flink-jobs/src/test/java/io/paystream/flink/rules/dynamic/ConformanceTest.java`
* Python: `rules-service/tests/test_conformance.py`

Event fields left out take these defaults: `txn_type` P2P, `sender_id` U1, `receiver_id` U2,
`amount_paisa` 100000, `device_id` D1, `district` Dhaka, `status` SUCCESS, `channel` APP, `currency` BDT.
`at` is the event time (UTC). Times in the descriptions are seconds after the first event.
