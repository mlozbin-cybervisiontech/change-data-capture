/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.cdc.source.salesforce.sobject;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Retrieves {@link DescribeSObjectResult}s for the given sObjects
 * and adds field information to the internal holder.
 * This class will be used to populate {@link SObjectDescriptor} for queries by sObject
 * or to generate CDAP schema based on Salesforce fields information.
 */
public class SObjectsDescribeResult {
  // key -> [sObject name], value -> [key -> field name,  value -> field]
  private final Map<String, Map<String, Field>> objectToFieldMap;


  public SObjectsDescribeResult(Map<String, Map<String, Field>> objectToFieldMap) {
    this.objectToFieldMap = new HashMap<>(objectToFieldMap);
  }

  /**
   * Retrieves all stored fields.
   *
   * @return list of {@link Field}s
   */
  public List<Field> getFields() {
    return objectToFieldMap.values().stream()
      .map(Map::values)
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  /**
   * Attempts to find {@link Field} by sObject name and field name.
   *
   * @param sObjectName sObject name
   * @param fieldName   field name
   * @return field instance if found, null otherwise
   */
  public Field getField(String sObjectName, String fieldName) {
    Map<String, Field> fields = objectToFieldMap.get(sObjectName.toLowerCase());
    return fields == null ? null : fields.get(fieldName.toLowerCase());
  }
}
