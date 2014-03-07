package level3;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 * Created by sujishakya on 3/7/14.
 */
public class TestLineToWord {
    @Test
    public void lineToWord(){
        //input of the job
        String sourcepath = "src/test/resources/level3.txt";
        Tap source = new FileTap(new TextLine(new Fields("line")),sourcepath);

        //output of the job
        String sinkpath = "src/test/resources/Outlevel3.txt";
        Tap sink = new FileTap(new TextLine(new Fields("word")), sinkpath);

        FlowDef flowDef = LineToWord.lineToWord(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
