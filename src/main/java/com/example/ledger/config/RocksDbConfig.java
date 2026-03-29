package com.example.ledger.config;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-tuned RocksDB configuration for Kafka Streams state stores.
 *
 * <h3>Tuning rationale</h3>
 * <ul>
 *   <li><b>Block cache (64 MB)</b> — keeps hot data in memory, reducing disk reads
 *       for interactive queries and stateful joins.</li>
 *   <li><b>Write buffer (16 MB × 3)</b> — absorbs write bursts before flushing to SST files.</li>
 *   <li><b>Bloom filter (10 bits/key)</b> — reduces false-positive lookups for large stores.</li>
 *   <li><b>Cache index/filter blocks</b> — pins index and bloom data in the block cache.</li>
 *   <li><b>LZ4 compression</b> — fast compression that reduces disk I/O.</li>
 *   <li><b>Universal compaction</b> — optimized for write-heavy streaming workloads.</li>
 * </ul>
 *
 * <p>Register via {@code rocksdb.config.setter} in the Kafka Streams properties.
 */
public class RocksDbConfig implements RocksDBConfigSetter {

    private static final Logger log = LoggerFactory.getLogger(RocksDbConfig.class);

    private static final long BLOCK_CACHE_SIZE = 64 * 1024 * 1024L;  // 64 MB
    private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L; // 16 MB
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final int BLOOM_FILTER_BITS = 10;

    private org.rocksdb.Cache cache;

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        log.info("Applying production RocksDB config for store: {}", storeName);

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        cache = new org.rocksdb.LRUCache(BLOCK_CACHE_SIZE);
        tableConfig.setBlockCache(cache);
        tableConfig.setBlockSize(4096);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        tableConfig.setFilterPolicy(new org.rocksdb.BloomFilter(BLOOM_FILTER_BITS, false));
        options.setTableFormatConfig(tableConfig);

        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setMinWriteBufferNumberToMerge(1);

        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setMaxBackgroundJobs(4);
    }

    @Override
    public void close(String storeName, Options options) {
        if (cache != null) {
            cache.close();
        }
    }
}

