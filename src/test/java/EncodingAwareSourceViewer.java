import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by king on 15-5-6.
 */
public class EncodingAwareSourceViewer {
    public final static String url = "http://dumps.wikimedia.org/enwiki/latest/";
    public static void main(String[] args) {
        String encoding = "UTF-8";
        try {
            URL u = new URL(url);
            URLConnection uc = u.openConnection();
            String contentType = uc.getContentType();
            int encodingStart = contentType.indexOf("charset=");
            if (encodingStart != -1)
                encoding = contentType.substring(encodingStart+8);
            BufferedReader r = new BufferedReader(new InputStreamReader(uc.getInputStream(), encoding));
            String line;
            while ( (line = r.readLine()) != null) {
                System.out.println(line);
            }
            r.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
