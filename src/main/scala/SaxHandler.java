import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SaxHandler extends DefaultHandler {

    private String author;

    private String content;

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch(qName){
            case "author":
                author = content;
                break;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        content = String.copyValueOf(ch, start, length).trim();
    }

    public String getAuthor() {
        return author;
    }
}
