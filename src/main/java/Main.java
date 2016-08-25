
public class Main {

    private static boolean isCorrect(String query) {
        return false;
    }

    private static int countVariables(String query) {
        int numberOfVariables = 0;
        return numberOfVariables;
    }

    private static String removeComments(String query) {
        return query;
    }

    public static void main(String[] args) {
        String query = "";
        System.out.println("Original Query: " + query);
        System.out.println("Is the query correct? " + isCorrect(query));
        System.out.println("How many variables does the query contain? " + countVariables(query));
        System.out.println("Query string with all comments removed: " + removeComments(query));
    }
}