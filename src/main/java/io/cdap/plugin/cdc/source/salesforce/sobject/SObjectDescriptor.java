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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains information about SObject, including its name and list of fields.
 */
public class SObjectDescriptor {

  private final String name;
  private final List<FieldDescriptor> fields;

  public SObjectDescriptor(String name, List<FieldDescriptor> fields) {
    this.name = name;
    this.fields = new ArrayList<>(fields);
  }

  public String getName() {
    return name;
  }

  /**
   * Collects sObject names needed to be described in order to obtains field type information.
   *
   * @return list of sObject names
   */
  public Set<String> getAllParentObjects() {
    Set<String> parents = fields.stream()
      .filter(FieldDescriptor::hasParents)
      .map(FieldDescriptor::getLastParent)
      .collect(Collectors.toSet());

    // add top level sObject for fields that don't have parents
    parents.add(name);

    return parents;
  }

  /**
   * Collects all field names, for fields with parents includes parents separated by dot.
   *
   * @return list of field names
   */
  public List<String> getFieldsNames() {
    return fields.stream()
      .map(FieldDescriptor::getFullName)
      .collect(Collectors.toList());
  }

  public List<FieldDescriptor> getFields() {
    return fields;
  }

  @Override
  public String toString() {
    return "SObjectDescriptor{" + "name='" + name + '\'' + ", fields=" + fields + '}';
  }

  /**
   * Contains information about field, including list of parents if present.
   */
  public static class FieldDescriptor {

    private final String name;
    private final List<String> parents;

    public FieldDescriptor(String name) {
      this.name = name;
      this.parents = new ArrayList<>();
    }

    public String getName() {
      return name;
    }

    /**
     * Returns field name with parents connected by dots.
     *
     * @return full field name
     */
    public String getFullName() {
      if (hasParents()) {
        List<String> nameParts = new ArrayList<>(parents);
        nameParts.add(name);
        return String.join(".", nameParts);
      }
      return name;
    }

    /**
     * Checks if field has parents.
     *
     * @return true if field has at least one parent, false otherwise
     */
    public boolean hasParents() {
      return !parents.isEmpty();
    }

    /**
     * Return last parent of the field.
     * Primary used to obtain describe result from Salesforce.
     *
     * @return last parent if field has parents, null otherwise
     */
    public String getLastParent() {
      return hasParents() ? parents.get(parents.size() - 1) : null;
    }

    @Override
    public String toString() {
      return "FieldDescriptor{" + "name='" + name + '\'' + ", parents=" + parents + '}';
    }
  }
}
