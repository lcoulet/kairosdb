/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.kairosdb.publisher.skyline;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.msgpack.MessagePack;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import org.kairosdb.core.DataPoint;
     

/**
 *
 * @author lcoulet
 */
public class SkylineDataPublisher implements DataPointListener{
    public static final Logger logger = LoggerFactory.getLogger(SkylineDataPublisher.class);
    public static final String PUBLISH_SKYLINE_TARGET_HOST="kairosdb.publisher.skyline.host";
    public static final String PUBLISH_SKYLINE_TARGET_PORT="kairosdb.publisher.skyline.port";

    private InetSocketAddress addr;
    private DatagramChannel datagramChannel;
    private MessagePack msgpack = new MessagePack();
    
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Inject
    public SkylineDataPublisher(@Named(PUBLISH_SKYLINE_TARGET_HOST) String target_host,
    @Named(PUBLISH_SKYLINE_TARGET_PORT) int target_port){
        addr=new InetSocketAddress(target_host, target_port);
        
        try{
            initialize();
        }catch(IOException e ){
            logger.error("Skyline Publisher Connection Error", e);
        }
    }
    
    private void initialize() throws IOException{
        datagramChannel = DatagramChannel.open();
        datagramChannel.configureBlocking(false);

        datagramChannel.register(Selector.open(), SelectionKey.OP_WRITE);
    }
    
    /**
     * Skyline metric name is an aggregated string of all the tags
     *  <metric>.<tag1_key>_<tag1_value>.<tag2_key>_<tag2_value>
     * Sophisticated rules can be added later on
     */
    protected  String buildSkylineMetricName(DataPointSet dps) {
        String slMetricName = dps.getName();

        for (String key : dps.getTags().keySet()) {
            slMetricName = slMetricName.concat("." + key + "_" +  dps.getTags().get(key));
        }
        
        return slMetricName.toString();
    }
        
    
    @Override
    public void dataPoints(DataPointSet dps) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    protected List buildMetricListForSkyline(DataPointSet dps){

            List metricList = new ArrayList();
                        
            String skylineMetricName=buildSkylineMetricName(dps);


            for (DataPoint d : dps.getDataPoints() ){
                metricList.add(skylineMetricName);
                metricList.add(Lists.newArrayList( d.getTimestamp(), d.getDoubleValue())) ;         
            }
            
            return metricList;

    }
    
    protected void publishToSkyline(DataPointSet dps) throws IOException{
        sendToSocket(buildMetricListForSkyline(dps));
    }
    
    protected void sendToSocket(Object value) throws IOException{
        datagramChannel.send(ByteBuffer.wrap(msgpack.write(value)), addr);
    }
    
    
}
