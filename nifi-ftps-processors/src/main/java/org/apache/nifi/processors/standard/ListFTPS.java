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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.FTPSTransfer;
import org.apache.nifi.processors.standard.util.FileTransfer;

import java.util.ArrayList;
import java.util.List;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "ftps", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on an FTPS server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
        + "set to the name of the file on the remote server. This can then be used in conjunction with FetchFTP in order to fetch those files.")
@SeeAlso({FetchFTPS.class, GetFTPS.class, PutFTPS.class})
@WritesAttributes({
        @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname of the FTPS Server"),
        @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was connected to on the FTPS Server"),
        @WritesAttribute(attribute = "ftp.listing.user", description = "The username of the user that performed the FTP Listing"),
        @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
        @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "filename", description = "The name of the file on the FTPS Server"),
        @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the FTPS Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
        + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListFTPS extends ListFileTransfer {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("21").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
//        properties.add(LISTING_STRATEGY);
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(FTPSTransfer.PASSWORD);
        properties.add(REMOTE_PATH);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(FTPSTransfer.RECURSIVE_SEARCH);
//        properties.add(FTPSTransfer.FOLLOW_SYMLINK);
        properties.add(FTPSTransfer.FILE_FILTER_REGEX);
        properties.add(FTPSTransfer.PATH_FILTER_REGEX);
        properties.add(FTPSTransfer.IGNORE_DOTTED_FILES);
        properties.add(FTPSTransfer.REMOTE_POLL_BATCH_SIZE);
        properties.add(FTPSTransfer.CONNECTION_TIMEOUT);
        properties.add(FTPSTransfer.DATA_TIMEOUT);
        properties.add(FTPSTransfer.CONNECTION_MODE);
        properties.add(FTPSTransfer.TRANSFER_MODE);
        properties.add(FTPSTransfer.BUFFER_SIZE);
        properties.add(TARGET_SYSTEM_TIMESTAMP_PRECISION);
//        properties.add(ListedEntityTracker.TRACKING_STATE_CACHE);
//        properties.add(ListedEntityTracker.TRACKING_TIME_WINDOW);
//        properties.add(ListedEntityTracker.INITIAL_LISTING_TARGET);
        properties.add(FTPSTransfer.ALLOW_SELFSIGNED);
        properties.add(FTPSTransfer.IMPLICIT);
        return properties;
    }

    @Override
    protected Scope getStateScope(ProcessContext processContext) {
        return null;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new FTPSTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "ftp";
    }

//    @Override
    protected Scope getStateScope(final PropertyContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

}
