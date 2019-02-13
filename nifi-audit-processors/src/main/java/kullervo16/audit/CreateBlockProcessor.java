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
package kullervo16.audit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

@Tags({"audit","blockchain","couchdb"})
@CapabilityDescription("Converts couchDB event stream(s) into audit blocks. It takes both a size and a duration, whichever is reached first will trigger the creation of a block")
public class CreateBlockProcessor extends AbstractProcessor {
    public static final String INTERVAL = "interval between blocks in minutes (default = 15)";
    public static final PropertyDescriptor PROPERTY_INTERVAL = new PropertyDescriptor.Builder()
            .name(INTERVAL)
            .defaultValue("15")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final String BLOCKSIZE = "maximum number of elements per block (default = 1000)";
    public static final PropertyDescriptor PROPERTY_BLOCKSIZE = new PropertyDescriptor.Builder()
            .name(BLOCKSIZE)
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();


    public static final Relationship SUCCESS_REL = new Relationship.Builder()
            .name("block")
            .build();



    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private long lastExecution = 0; // TODO : from state
    private int blockNumber = 0; // TODO : from state
    private String lastHash = "root"; // TODO : from state


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_BLOCKSIZE);
        descriptors.add(PROPERTY_INTERVAL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_REL);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if(!conditionsMet(context, session)) {
            return;
        }

        FlowFile incomingFlowFile = session.get();
        if ( incomingFlowFile == null ) {
            // no data, so just update the execution time
            this.lastExecution = System.currentTimeMillis(); // TODO : set state
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        FlowFile outgoingFlowFile = session.create();
        try(OutputStream os = session.write(outgoingFlowFile);
            GZIPOutputStream gzos = new GZIPOutputStream(os);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(gzos))) {
            StringBuilder blockContents = new StringBuilder("Hash of previous block : ");
            blockContents.append(this.lastHash).append("\n");
            blockContents.append("Block created at ").append(sdf.format(new Date())).append("\n");
            while (incomingFlowFile != null) {

                long time = incomingFlowFile.getLineageStartDate();
                blockContents.append(""+time).append("|");
                blockContents.append(sdf.format(new Date(time))).append("|");
                blockContents.append(incomingFlowFile.getAttribute("event.source")).append("|");
                blockContents.append(incomingFlowFile.getAttribute("doc.id")).append("|");
                blockContents.append(incomingFlowFile.getAttribute("doc.rev")).append("|");
                // only the sequence number is guaranteed, when using another full sequence number in the since field,
                // you will get another ordering... so just track the size of the stream
                blockContents.append(incomingFlowFile.getAttribute("event.sequence").split("-")[0]).append("\n");

                session.remove(incomingFlowFile);
                incomingFlowFile = session.get();
            }
            String content = blockContents.toString();
            writer.write(content);
            // hash of previous block is part of it... this constructs the chain
            this.lastHash = DigestUtils.sha512Hex(content); // TODO : set state
            writer.write(lastHash);
            writer.write("\n");
            writer.flush();
            writer.close();
            gzos.close();
            os.close();
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, CoreAttributes.FILENAME.key(), "block_"+blockNumber);
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, CoreAttributes.MIME_TYPE.key(), "application/gzip");
            // TODO : set state
            blockNumber++;
            session.transfer(outgoingFlowFile, SUCCESS_REL);
            this.lastExecution = System.currentTimeMillis(); // TODO : set state
        }catch(IOException ioe) {
            getLogger().error(ioe.getMessage(), ioe);
            session.rollback();
        }
    }

    /**
     * We execute when either the number of waiting elements is larger than the requested blocksize or when the last
     * execution was longer ago than the required interval (in which case we may create a partial block)
     * @param context
     * @param session
     * @return
     */
    private boolean conditionsMet(ProcessContext context, ProcessSession session) {
        return session.getQueueSize().getObjectCount() > context.getProperty(BLOCKSIZE).asInteger() ||
                (System.currentTimeMillis() - this.lastExecution) > context.getProperty(INTERVAL).asInteger() * 1000 * 60;
    }
}
