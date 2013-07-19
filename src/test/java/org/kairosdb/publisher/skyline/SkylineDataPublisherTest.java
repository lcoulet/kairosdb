/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.kairosdb.publisher.skyline;

import org.msgpack.template.Templates;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javassist.bytecode.ByteArray;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import static org.junit.Assert.*;
import org.kairosdb.core.DataPointSet;
import org.msgpack.MessagePack;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;
import org.msgpack.template.Template;
import org.msgpack.unpacker.BufferUnpacker;

        
/**
 *
 * @author lcoulet
 */
public class SkylineDataPublisherTest {
    private final int UDP_PORT_NUMBER = 18200;
    private final String testMetricName = "MetricToDelete";
    
    public SkylineDataPublisherTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of dataPoints method, of class SkylineDataPublisher.
     */
    @Test
    public void testBuildSkylineMetricName() {        
        DataPointSet dpSet = new DataPointSet(testMetricName);
        dpSet.addTag("host", "foo");
        dpSet.addTag("client", "bar");
        dpSet.addTag("system", "kix");
        SkylineDataPublisher instance=minimalTestInstance();
        assertEquals("Build Metric Name", "MetricToDelete.client_bar.host_foo.system_kix", instance.buildSkylineMetricName(dpSet));
        dpSet.addTag("astral-sign", "lion");
        assertEquals("Build Metric Name", "MetricToDelete.astral-sign_lion.client_bar.host_foo.system_kix", instance.buildSkylineMetricName(dpSet));
        
    }
    
    @Test
    public void testBuildMetricListForSkyline(){
        DataPointSet dpSet = new DataPointSet(testMetricName);
        dpSet.addDataPoint(new DataPoint(0L, 1.0));
        
        
        
        List expected = new ArrayList();
        expected.add(testMetricName);
        expected.add(Lists.newArrayList(0L, 1.0));
        
        SkylineDataPublisher instance = minimalTestInstance();
        
        assertThat(instance.buildMetricListForSkyline(dpSet), is(equalTo(expected)));
        
    }
    
    @Test
    public void testPublishingMetricListForSkyline() throws InterruptedException{
        
        try{
            DatagramChannel channel = DatagramChannel.open();
            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(UDP_PORT_NUMBER));
            ByteBuffer buf = ByteBuffer.allocate(1024);
            channel.receive(buf);
            buf.clear();
            
            
            DataPointSet dpSet = new DataPointSet(testMetricName);
            dpSet.addDataPoint(new DataPoint(0L, 1.0));
            
            SkylineDataPublisher instance = minimalTestInstance();
        
            instance.publishToSkyline(dpSet);
                        
            Thread.sleep(100);
                       
            channel.receive(buf);  
            buf.flip();
                        
            channel.close();
            
            MessagePack msgpack = new MessagePack();
            Template listTmpl = Templates.tList(Templates.TValue);            
            BufferUnpacker unpacker = msgpack.createBufferUnpacker(buf.array());                        
            
            // I compare the data read from UDP socket with data unpacked from packed metrics list
            List unpacked = (ArrayList) unpacker.read(listTmpl);
            List expected = (ArrayList) msgpack.read(msgpack.write(instance.buildMetricListForSkyline(dpSet)),listTmpl);
            
            assertThat(unpacked, is(equalTo(expected)));
            
            
            
        }catch(IOException e){
            fail("Exception caught during test: " + e.toString());
        }
        
    }
    
    

    private SkylineDataPublisher minimalTestInstance() {
        SkylineDataPublisher instance = new SkylineDataPublisher("localhost", UDP_PORT_NUMBER);
        return instance;
    }
}
