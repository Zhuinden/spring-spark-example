package com.zhuinden.sparkexperiment;

/**
 * Created by Owner on 2017. 03. 29..
 */
public class Count {
    private String word;
    private long count;

    public Count() {
    }

    public Count(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
