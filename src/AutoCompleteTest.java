import java.util.ArrayList;

/**
 * @author Siqi Wang siqiw1 on 4/8/16.
 */
public class AutoCompleteTest {
    @org.junit.Test
    public void formatKVpair() throws Exception {
        String line = "carnegie\t4";
        ArrayList<AutoComplete.KVPair> list = AutoComplete.formatKVpair(line);
        for (AutoComplete.KVPair kvPair : list) {
            System.out.println(kvPair.getKey() + "\t" + kvPair.getValue());
        }
    }

}