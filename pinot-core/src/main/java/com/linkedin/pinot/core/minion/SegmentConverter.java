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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.minion.segment.DefaultRecordPartitioner;
import com.linkedin.pinot.core.minion.segment.MapperRecordReader;
import com.linkedin.pinot.core.minion.segment.RecordAggregator;
import com.linkedin.pinot.core.minion.segment.RecordPartitioner;
import com.linkedin.pinot.core.minion.segment.RecordTransformer;
import com.linkedin.pinot.core.minion.segment.ReducerRecordReader;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Genenric Segment Converter
 */
public class SegmentConverter {
  private static final int DEFAULT_NUM_PARTITION = 1;

  // Required
  private List<File> _inputIndexDirs;
  private File _workingDir;
  private String _tableName;
  private String _segmentName;
  private RecordTransformer _recordTransformer;

  // Use default values
  private RecordPartitioner _recordPartitioner;
  private int _totalNumPartition;

  // Optional but these 2 needs to be configured together
  private RecordAggregator _recordAggregator;
  private List<String> _aggregationColumns;

  // Optional
  private IndexingConfig _indexingConfig;

  public SegmentConverter(@Nonnull List<File> inputIndexDirs, @Nonnull File workingDir, @Nonnull String tableName,
      @Nonnull String segmentName, int totalNumPartition, @Nonnull RecordTransformer recordTransformer,
      @Nullable RecordPartitioner recordPartitioner, @Nullable RecordAggregator recordAggregator,
      @Nullable List<String> aggregationColumns, @Nullable IndexingConfig indexingConfig) {
    _inputIndexDirs = inputIndexDirs;
    _workingDir = workingDir;
    _recordTransformer = recordTransformer;
    _tableName = tableName;
    _segmentName = segmentName;

    _recordPartitioner = (recordPartitioner == null) ? new DefaultRecordPartitioner() : recordPartitioner;
    _totalNumPartition = (totalNumPartition < 1) ? DEFAULT_NUM_PARTITION : totalNumPartition;

    _recordAggregator = recordAggregator;
    _aggregationColumns = aggregationColumns;
    _indexingConfig = indexingConfig;
  }

  public List<File> convertSegment() throws Exception {
    List<File> resultFiles = new ArrayList<>();
    for (int currentPartition = 0; currentPartition < _totalNumPartition; currentPartition++) {
      // Mapping stage
      Preconditions.checkNotNull(_recordTransformer);
      String mapperOutputPath = _workingDir.getPath() + File.separator + "mapper_" + currentPartition;
      try (MapperRecordReader mapperRecordReader = new MapperRecordReader(_inputIndexDirs, _recordTransformer,
          _recordPartitioner, _totalNumPartition, currentPartition)) {
        buildSegment(mapperOutputPath, _tableName, _segmentName, mapperRecordReader, null);
      }
      File outputSegment = new File(mapperOutputPath + File.separator + _segmentName);

      // Sorting on aggregation column & reduce
      if (_recordAggregator != null && _aggregationColumns != null && _aggregationColumns.size() > 0) {
        String reducerOutputPath = _workingDir.getPath() + File.separator + "reducer_" + currentPartition;
        try (ReducerRecordReader reducerRecordReader = new ReducerRecordReader(outputSegment, _recordAggregator,
            _aggregationColumns)) {
          buildSegment(reducerOutputPath, _tableName, _segmentName, reducerRecordReader, null);
        }
        outputSegment = new File(reducerOutputPath + File.separator + _segmentName);
      }

      // Sorting on sorted column and creating indices
      if (_indexingConfig != null) {
        List<String> sortedColumn = _indexingConfig.getSortedColumn();
        StarTreeIndexSpec starTreeIndexSpec = _indexingConfig.getStarTreeIndexSpec();
        List<String> invertedIndexColumns = _indexingConfig.getInvertedIndexColumns();

        // Check if the table config has any index configured
        if ((sortedColumn != null && !sortedColumn.isEmpty()) || starTreeIndexSpec != null
            || invertedIndexColumns != null) {
          String indexGenerationOutputPath = _workingDir.getPath() + File.separator + "final_" + currentPartition;
          try (
              PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(outputSegment, null, sortedColumn)) {
            buildSegment(indexGenerationOutputPath, _tableName, _segmentName, recordReader, _indexingConfig);
          }
          outputSegment = new File(indexGenerationOutputPath + File.separator + _segmentName);
        }
      }

      resultFiles.add(outputSegment);
    }
    return resultFiles;
  }

  /**
   * Helper function to trigger the segment creation
   */
  private void buildSegment(String outputPath, String tableName, String segmentName, RecordReader recordReader,
      IndexingConfig indexingConfig) throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(recordReader.getSchema());
    segmentGeneratorConfig.setOutDir(outputPath);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setSegmentName(segmentName);
    if (indexingConfig != null) {
      segmentGeneratorConfig.setInvertedIndexCreationColumns(indexingConfig.getInvertedIndexColumns());
      if (indexingConfig.getStarTreeIndexSpec() != null) {
        segmentGeneratorConfig.enableStarTreeIndex(indexingConfig.getStarTreeIndexSpec());
      }
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
  }
}
