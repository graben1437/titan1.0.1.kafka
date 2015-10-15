package com.david.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.log.*;
import com.thinkaurelius.titan.graphdb.relations.SimpleTitanProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by robinson on 10/1/15.
 */
public class LogReader
{
    public static void main(String args[])
    {
        LogReader lr = new LogReader();
        lr.doWork();
    }

    private void doWork()
    {

        TitanGraph graph = TitanFactory.open("/home/graphie/titankafka/bin/cass.properties");
        LogProcessorFramework logProcessor = TitanFactory.openTransactionLog(graph);

        // listens on a user defined log called david1
        logProcessor.addLogProcessor("david1").
                setProcessorIdentifier("david1reader").
                setStartTime(Instant.now()).
                addProcessor(
                        new ChangeProcessor()
                {
                    @Override
                    public void process(TitanTransaction tx, TransactionId txId, ChangeState changeState)
                    {
                        Path path = Paths.get("KCVSLogProcessor.txt");
                        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.APPEND))
                        {
                            // writer.write("msg: " + changeState.toString() + "\n");
                            // writer.write(Arrays.toString(Thread.currentThread().getStackTrace()) + "\n");

                            Set<TitanVertex> vertexSet = changeState.getVertices(Change.ANY);
                            for (TitanVertex v : vertexSet)
                            {
                                writer.write("vertex label: " + v.label() + "\n");
                                Iterator vertProps = v.properties();
                                while (vertProps.hasNext())
                                {
                                    // writer.write("vertex property type: " + vertProps.next().getClass() + "\n");
                                    // class com.thinkaurelius.titan.graphdb.relations.CacheVertexProperty
                                    VertexProperty vp = (VertexProperty)vertProps.next();
                                    // writer.write("together: " + vp.toString() + "\n");
                                    writer.write("key: " + vp.key() + " value: " + vp.value() + "\n");

                                }

                                Iterable<TitanEdge> edgeItr = changeState.getEdges(v, Change.ANY, Direction.OUT);
                                for (Iterator<TitanEdge> anEdge = edgeItr.iterator(); anEdge.hasNext(); )
                                {
                                    TitanEdge edge = anEdge.next();
                                    // edge.properties puts out an interable
                                    writer.write("edge label: " + edge.edgeLabel() + "\n");
                                    Iterator propsItr = edge.properties();
                                    while (propsItr.hasNext())
                                    {
                                        // writer.write("edge property type: " + propsItr.next().getClass() + "\n");
                                        SimpleTitanProperty ep = (SimpleTitanProperty)propsItr.next();
                                        // writer.write("together: " + ep.toString() + "\n");
                                        writer.write("edge property: " + "key: " + ep.propertyKey() + " value: " + ep.value() + "\n");
                                    }
                                }
                            }
                        } catch (Exception e)
                        {
                            e.printStackTrace();
                        }


                    }
                }).build();
    }
}
