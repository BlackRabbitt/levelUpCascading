package level1;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import level0.CopyFiles;
import org.junit.Test;

/**
 * Created by sujit shakya on 3/7/14.
 */
public class TestJoin {
    @Test
    public void testJoin() throws Exception{
        // define source
        String source1File = "src/test/resources/level1-1.csv";
        String source2File = "src/test/resources/level1-2.csv";
        Tap source1Tap = new FileTap(new TextDelimited(new Fields("id","name"), ";"), source1File);
        Tap source2Tap = new FileTap(new TextDelimited(new Fields("id","college"), ";"), source2File);

        // define sink
        String sinkFile = "src/test/resources/Outlevel1.csv";

        Tap sinkTap = new FileTap(new TextLine(), sinkFile);

        // create a job definition and run it
        FlowDef flowDef = JoinLearn.JoinFiles(source1Tap, source2Tap, sinkTap);
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
