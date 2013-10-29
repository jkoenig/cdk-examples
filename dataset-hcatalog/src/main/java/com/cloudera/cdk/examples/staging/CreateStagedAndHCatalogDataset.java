/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.examples.staging;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.Formats;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;

public class CreateStagedAndHCatalogDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    FileSystemDatasetRepository fileRepo = new FileSystemDatasetRepository.Builder()
        .rootDirectory(new Path("/tmp/data"))
        .get();

    // create an avro dataset to temporarily hold data
    fileRepo.create("logs-staging", new DatasetDescriptor.Builder()
        .format(Formats.AVRO)
        .schemaUri("resource:simple-log.avsc")
        .partitionStrategy(new PartitionStrategy.Builder()
            .day("timestamp", "day")
            .get())
        .get());
    
    // create a HCatalog dataset repository using managed Hive tables
    DatasetRepository hCatalogRepo = new HCatalogDatasetRepository.Builder().get();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
    .format(Formats.AVRO)
    .schemaUri("resource:simple-log.avsc")
    .partitionStrategy(new PartitionStrategy.Builder()
        .year("timestamp", "year")
        .month("timestamp", "month")
        .day("timestamp", "day")
        .get()).get();
    
    hCatalogRepo.create("logs", descriptor);

    return 0;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new CreateStagedAndHCatalogDataset(), args);
    System.exit(rc);
  }
}
