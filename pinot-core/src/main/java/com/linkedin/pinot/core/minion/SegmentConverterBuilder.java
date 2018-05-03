/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.minion;

import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.core.minion.segment.RecordAggregator;
import com.linkedin.pinot.core.minion.segment.RecordPartitioner;
import com.linkedin.pinot.core.minion.segment.RecordTransformer;
import java.io.File;
import java.util.List;


/**
 * Builder for segment converter
 */
public class SegmentConverterBuilder {
  private List<File> _inputIndexDirs;
  private File _workingDir;
  private String _tableName;
  private String _segmentName;
  private RecordTransformer _recordTransformer;
  private RecordPartitioner _recordPartitioner;
  private int _totalNumPartition;
  private RecordAggregator _recordAggregator;
  private List<String> _aggregationColumns;
  private IndexingConfig _indexingConfig;

  public SegmentConverterBuilder() {
  }

  public SegmentConverterBuilder setInputIndexDirs(List<File> inputIndexDirs) {
    _inputIndexDirs = inputIndexDirs;
    return this;
  }

  public SegmentConverterBuilder setWorkingDir(File workingDir) {
    _workingDir = workingDir;
    return this;
  }

  public SegmentConverterBuilder setRecordPartitioner(RecordPartitioner recordPartitioner) {
    _recordPartitioner = recordPartitioner;
    return this;
  }

  public SegmentConverterBuilder setRecordTransformer(RecordTransformer recordTransformer) {
    _recordTransformer = recordTransformer;
    return this;
  }

  public SegmentConverterBuilder setRecordAggregator(RecordAggregator recordAggregator) {
    _recordAggregator = recordAggregator;
    return this;
  }

  public SegmentConverterBuilder setTotalNumPartition(int totalNumPartition) {
    _totalNumPartition = totalNumPartition;
    return this;
  }

  public SegmentConverterBuilder setAggregationColumns(List<String> aggregationColumns) {
    _aggregationColumns = aggregationColumns;
    return this;
  }

  public SegmentConverterBuilder setIndexingConfig(IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
    return this;
  }

  public SegmentConverterBuilder setSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  public SegmentConverterBuilder setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public SegmentConverter build() {
    return new SegmentConverter(_inputIndexDirs, _workingDir, _tableName, _segmentName, _totalNumPartition,
        _recordTransformer, _recordPartitioner, _recordAggregator, _aggregationColumns, _indexingConfig);
  }
}
