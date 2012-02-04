#### Simple Aggregator for Storm

This is a set of simple demos of Storm indicating how it can be used for aggregation of events and
for Bayesian on-line learning

See SnappedCounterTest for a beginning of a test for the counter.  This test uses EventSpout
to create a stream of keys and values, SnappedCounter to count them and FileBolt to persist
them.

Note the use of tuple acking to avoid any sort of retry log in the counter.

#### Beta Bayesian Bandit Model
The BanditTrainer shows how a two-armed bandit can be solved using a model that I call the
beta-Bayesian model.

#### Beta Distributed Random Walk

The BetaWalk implements a random walk that has assymptotic beta distribution.  This is useful for
modeling conversion probabilities that vary in time but which have realistic distribution.