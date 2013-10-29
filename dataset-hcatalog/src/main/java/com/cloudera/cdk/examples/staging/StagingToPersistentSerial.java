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

import java.util.Calendar;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;

public class StagingToPersistentSerial extends Configured implements Tool {

	public static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000;


	@Override
	public int run(String[] args) throws Exception {
		// open the repository
		final DatasetRepository fileRepo = new FileSystemDatasetRepository.Builder()
				.rootDirectory(new Path("/tmp/data")).get();

		final Calendar yesterdayTs = Calendar.getInstance();
		yesterdayTs.setTimeInMillis(System.currentTimeMillis() - DAY_IN_MILLIS);

		// the destination dataset
		DatasetRepository hCatalogRepo = new HCatalogDatasetRepository.Builder()
				.get();
		final Dataset table = hCatalogRepo.load("logs");
		final DatasetWriter<GenericRecord> writer = table.newWriter();
		writer.open();

		// the source dataset: yesterday's partition in the staging area
		final Dataset staging = fileRepo.load("logs-staging");
		Marker marker = new Marker.Builder()
		     .add("day",yesterdayTs.get(Calendar.DAY_OF_MONTH))
		     .get();
		 
		final PartitionKey yesterday = staging.getDescriptor().getPartitionStrategy().keyFor(marker);
		final DatasetReader<GenericRecord> reader = staging.getPartition(
				yesterday, false).newReader();

		final GenericRecordBuilder builder = new GenericRecordBuilder(
				table.getDescriptor().getSchema());
		try {
			reader.open();

			// yep, it's that easy.
			for (GenericRecord record : reader) {
				builder.set("timestamp", record.get("timestamp"));
				builder.set("component", record.get("component"));
				builder.set("level", record.get("level"));
				builder.set("message", record.get("message"));
				writer.write(builder.build());
			}

		} finally {
			reader.close();
			writer.flush();
		}

		// remove the source data partition from staging
		staging.dropPartition(yesterday);

		// if the above didn't throw an exception, commit the data
		writer.close();

		return 0;
	}

	public static void main(String... args) throws Exception {
		int rc = ToolRunner.run(new StagingToPersistentSerial(), args);
		System.exit(rc);
	}
}
