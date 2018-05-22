import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.util.*;
import edu.stanford.nlp.ling.*;
import java.util.*;

public class NameThatEntity {

    protected StanfordCoreNLP pipeline;

    /**
     * Constructs the annotator pipeline for NER.
     * Must be called only once in the program.
     */
    public void initPipeline() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        this.pipeline = new StanfordCoreNLP(props);
    }


    /**
     * Passes the cleaned sentences through the pipeline to be annotated. If
     * the text is not a Tweet, it can be used directly.
     *
     * @param text
     * @return Returns the list of token - types pairs.
     */
    public ArrayList<NamedEntity> tagIt(String text) {
        ArrayList<NamedEntity> list = new ArrayList<>();
        CoreLabel cur_token = null;
        String prev_token = null;
        Annotation document = new Annotation(text);
        this.pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                cur_token = token;
                list = nerIt(cur_token, prev_token, list);
                prev_token = token.get(NamedEntityTagAnnotation.class);
            }
        }

        return list;
    }

    /**
     * Constructs the list of token - type pairs.
     */
    private ArrayList<NamedEntity> nerIt(CoreLabel cur_token, String prev_token, ArrayList<NamedEntity> list) {
        String[] ann = new String[] {  "PERSON", "ORGANIZATION", "COUNTRY" };

        // Add into the if: !cur_token.get(NamedEntityTagAnnotation.class).equals("***") to exclude type *** from NEs
        String entity = cur_token.get(NamedEntityTagAnnotation.class);
        if (Arrays.asList(ann).contains(entity)) {
            if (entity.equals("COUNTRY")) entity = "LOCATION";

            if (entity.equals(prev_token)) { // curr Token has type same as previous Type
                int last_index = list.size() - 1;
                String last_word = list.remove(last_index).getToken() + " " + cur_token.originalText();
                list.add(new NamedEntity(last_word, entity));
            } else {
                list.add(new NamedEntity(cur_token.originalText(), entity));
            }
        }
        return list;
    }
}

