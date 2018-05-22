public class NamedEntity {
    protected String token;
    protected String entity;

    protected NamedEntity(String token, String entity) {
        this.token = token;
        this.entity = entity;
    }

    protected String getToken() {
        return token;
    }

    protected String getEntity() {
        return entity;
    }
}
