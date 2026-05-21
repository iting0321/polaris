/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.spark.exceptions;

import org.apache.polaris.core.exceptions.PolarisException;
import org.apache.spark.sql.connector.catalog.Identifier;

public class PaimonRegistrationException extends PolarisException {
  public PaimonRegistrationException(
      Identifier ident, Exception originalException, boolean createdPaimonTable) {
    super(message(ident, createdPaimonTable), originalException);
  }

  private static String message(Identifier ident, boolean createdPaimonTable) {
    String paimonState =
        createdPaimonTable
            ? "The Paimon table was created successfully"
            : "The Paimon table already exists";
    return String.format(
        "%s, but Polaris registration failed for %s. Please retry the create table request; "
            + "the retry will reuse the existing Paimon table and register it in Polaris.",
        paimonState, ident);
  }
}
