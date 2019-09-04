/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import static com.github.jcustenborder.kafka.connect.utils.AssertSchema.assertSchema;
import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class TrimStringTest extends TransformationTest {
  protected TrimStringTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void test() {
    this.transformation.configure(
        ImmutableMap.of(TrimStringConfig.FIELD_CONFIG, "FIRST_NAME,LAST_NAME")
    );
    final Schema inputSchema = SchemaBuilder.struct()
        .field("FIRST_NAME", Schema.STRING_SCHEMA)
        .field("LAST_NAME", Schema.STRING_SCHEMA)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("FIRST_NAME", Schema.STRING_SCHEMA)
        .field("LAST_NAME", Schema.STRING_SCHEMA)
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("FIRST_NAME", "test    ")
        .put("LAST_NAME", "         user");
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("FIRST_NAME", "test")
        .put("LAST_NAME", "user");

    final SinkRecord inputRecord = new SinkRecord(
        "topic",
        1,
        null,
        null,
        inputSchema,
        inputStruct,
        1L
    );
    for (int i = 0; i < 50; i++) {
      final SinkRecord transformedRecord = this.transformation.apply(inputRecord);
      assertNotNull(transformedRecord, "transformedRecord should not be null.");
      assertSchema(expectedSchema, transformedRecord.valueSchema());
      assertStruct(expectedStruct, (Struct) transformedRecord.value());
    }

  }


  public static class ValueTest<R extends ConnectRecord<R>> extends TrimStringTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new TrimString.Value<>();
    }
  }
}
