import java.text.SimpleDateFormat;
import java.util.Date;

public class JavaTest {
    public static void main(String[] args) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        Date date = new Date(System.currentTimeMillis());
        String day = simpleDateFormat.format(1215878400L * 1000L);
        System.out.println(day);
        System.out.println(System.currentTimeMillis());
    }
}
