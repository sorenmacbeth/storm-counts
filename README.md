#### Simple Aggregator for Storm

This is a simple demo of Storm indicating how it can be used for clean counting of events.

See SnappedCounterTest for a beginning of a test for the counter.  This test uses EventSpout
to create a stream of keys and values, SnappedCounter to count them and FileBolt to persist
them.

Note the use of tuple acking to avoid any sort of retry log in the counter.