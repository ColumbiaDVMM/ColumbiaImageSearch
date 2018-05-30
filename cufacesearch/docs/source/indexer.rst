Indexer
=======

An indexer server as the backend of the pipeline, it would store feature for the images
and batch of images update information.
Currently only the HBase Indexer is implemented but other database could be used as backend
as long as a class provide similar methods.

HBase Indexer
-------------

.. automodule:: cufacesearch.indexer.hbase_indexer_minimal
      :members: HBaseIndexerMinimal
