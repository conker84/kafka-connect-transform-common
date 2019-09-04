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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class TrimString<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(TrimString.class);

  @Override
  public ConfigDef config() {
    return TrimStringConfig.config();
  }

  TrimStringConfig config;

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new TrimStringConfig(settings);
  }

  @Override
  public void close() {

  }

  @Override
  protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
    return new SchemaAndValue(inputSchema, input.trim());
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    Struct struct = new Struct(inputSchema);
    for (Field field : inputSchema.fields()) {
      if (this.config.fields.contains(field.name())) {
        String inputString = input.getString(field.name());
        struct.put(field.name(), inputString.trim());
      } else {
        struct.put(field.name(), input.get(field.name()));
      }
    }
    return new SchemaAndValue(inputSchema, struct);
  }

  @Title("TrimString(Key)") 
  @Description("This transformation is used to trim a string.")
  @DocumentationTip("This transformation is used to trim fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends TrimString<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("TrimString(Value)")
  @Description("This transformation is used to trim a string.")
  public static class Value<R extends ConnectRecord<R>> extends TrimString<R> {
    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }
}
