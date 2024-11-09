/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.fluentd.ingester;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Response {
  private final String ack;

  public Response(@JsonProperty("ack") String ack) {
    this.ack = ack;
  }

  @JsonProperty("ack")
  public String getAck() {
    return ack;
  }

  @Override
  public String toString() {
    return "ResponseOption{" + "ack=" + ack + '}';
  }
}
