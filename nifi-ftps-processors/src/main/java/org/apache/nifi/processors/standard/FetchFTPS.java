package org.apache.nifi.processors.standard;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FTPSTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;

import java.util.ArrayList;
import java.util.List;


// Note that we do not use @SupportsBatching annotation. This processor cannot support batching because it must ensure that session commits happen before remote files are deleted.
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"ftps", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote FTPS server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
@WritesAttributes({
        @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname or IP address from which the file was pulled"),
        @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was used to communicate with the remote FTPS server"),
        @WritesAttribute(attribute = "ftp.remote.filename", description = "The name of the remote file that was pulled"),
        @WritesAttribute(attribute = "filename", description = "The filename is updated to point to the filename fo the remote file"),
        @WritesAttribute(attribute = "path", description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute")
})
public class FetchFTPS extends FetchFileTransfer {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("21").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(FTPSTransfer.PASSWORD);
        properties.add(REMOTE_FILENAME);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        properties.add(MOVE_CREATE_DIRECTORY);
        properties.add(FTPSTransfer.CONNECTION_TIMEOUT);
        properties.add(FTPSTransfer.DATA_TIMEOUT);
        properties.add(FTPSTransfer.USE_COMPRESSION);
        properties.add(FTPSTransfer.CONNECTION_MODE);
        properties.add(FTPSTransfer.TRANSFER_MODE);
        properties.add(FTPSTransfer.BUFFER_SIZE);
        //properties.add(FILE_NOT_FOUND_LOG_LEVEL);
        properties.add(FTPSTransfer.ALLOW_SELFSIGNED);
        properties.add(FTPSTransfer.IMPLICIT);
        return properties;
    }

    @Override
    protected FileTransfer createFileTransfer(final ProcessContext context) {
        return new FTPSTransfer(context, getLogger());
    }

}
