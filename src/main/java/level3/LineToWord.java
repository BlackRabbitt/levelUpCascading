package level3;

import cascading.flow.FlowDef;
import cascading.operation.Function;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Created by sujishakya on 3/7/14.
 */
public class LineToWord {
    public static FlowDef lineToWord(Tap source, Tap sink) {
        Pipe pipe = new Pipe("pipe");
        // Define the regex for the split job of lines.
        // The more efficient regex the more efficient your program is.
        String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

        // Function split is defined that takes input a line and output the list of words in new field word.
        // Regex generator will emit a new Tuple for every matched regular expression group.
        Function split = new RegexGenerator(new Fields("word"), regex);

        //select each tuple stream from line Field and apply split function to those stream.
        pipe = new Each(pipe, new Fields("line"), split);

        return FlowDef.flowDef()
                .addSource(pipe,source)
                .addTail(pipe)
                .addSink(pipe,sink);
    }
}
