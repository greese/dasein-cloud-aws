/**
 * Copyright (C) 2009-2015 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
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
 * ====================================================================
 */

package org.dasein.cloud.aws.compute;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Interface to be used for stream parser implementations.
 * <p>Created by Stas Maksimov: 11/04/2014 19:13</p>
 *
 * @author Stas Maksimov
 * @version 2014.03 initial version
 * @since 2014.03
 * @see org.dasein.cloud.aws.compute.EC2Method#invoke(XmlStreamParser)
 */
public interface XmlStreamParser<T> {

    List<T> parse(InputStream stream) throws IOException, CloudException, InternalException;

}
