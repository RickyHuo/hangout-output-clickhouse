import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huochen on 2017/9/26.
 */
public class TestMain {
    public static void main(String args[]) {

        String src = "a='b+\\'+c'";
        System.out.println(src);
        System.out.println(src.replace("'", "\\'"));
        System.out.println(src.replace("'", "\\'").replace("\\\\'", "\\\\\\'"));
    }
}
