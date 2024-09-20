from datachain import DataChain

(
  DataChain
  .from_storage("s3://commoncrawl/contrib/datacomp/DCLM-baseline/")
  .save("index")
)

