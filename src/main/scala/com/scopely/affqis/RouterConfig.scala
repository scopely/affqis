/*
 *    Copyright 2015 Scopely
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.scopely.affqis

import scala.util.Try

/**
 * Configuration from environment variables.
 */
object RouterConfig {
  val port: Int = Try(Integer.parseInt(System.getenv("PORT"))).getOrElse(8080)
  val host: String = Option(System.getenv("HOST")).getOrElse("0.0.0.0")
  val url: String = s"ws://$host:$port/affqis"
}
