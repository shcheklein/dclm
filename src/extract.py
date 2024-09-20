import io

from datachain import DataChain, File

from collections.abc import Iterator

import zstandard as zstd


def extract(file: File | str) -> Iterator[str]:
    with file.open() if isinstance(file, File) else open(file, "rb") as f:
        dctx = zstd.ZstdDecompressor()
        stream_reader = dctx.stream_reader(f)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        for line in text_stream:
            yield line


dcs = DataChain.from_dataset("index").settings(cache=True).limit(1).gen(extract, output={"json": str})

dcs.show(3)

#for l in extract("s3_shard_00000000_processed.jsonl.zstd"):
#    print(l)
#    break

