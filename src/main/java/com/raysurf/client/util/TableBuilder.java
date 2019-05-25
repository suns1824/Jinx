package com.raysurf.client.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TableBuilder {
    private final int columns;
    private final List<String[]> rows = new LinkedList();
    private boolean customSplitLine = false;

    public TableBuilder(int columns) {
        this.columns = columns;
    }

    public void addRow(String... cols) {
        this.rows.add(cols);
    }

    /** @deprecated */
    @Deprecated
    public void setHeader(String[] cols, String splitStr) {
        this.addSplitLine(splitStr);
        this.addRow(cols);
        this.addSplitLine(splitStr);
        this.customSplitLine = true;
    }

    public void setHeader(String[] cols) {
        this.addRow(cols);
    }

    public void addSplitLine(String str) {
        String[] arr = new String[this.columns];

        for(int i = 0; i < this.columns; ++i) {
            arr[i] = str;
        }

        this.addRow(arr);
    }

    private int[] colWidths() {
        int cols = 0;
        String[] row;
        for(Iterator itr = this.rows.iterator(); itr.hasNext(); cols = Math.max(cols, row.length)) {
            row = (String[])itr.next();
        }

        int[] widths = new int[cols];
        Iterator itr = this.rows.iterator();

        while(itr.hasNext()) {
            row = (String[])itr.next();

            for(int colNum = 0; colNum < row.length; ++colNum) {
                widths[colNum] = Math.max(widths[colNum], TextUtils.length(row[colNum]));
            }
        }

        return widths;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        int[] colWidths = this.colWidths();
        if (colWidths.length <= 0) {
            return "";
        } else {
            String[] line;
            if (this.customSplitLine) {
                Iterator itr = this.rows.iterator();

                while(itr.hasNext()) {
                    line = (String[])itr.next();
                    if (line != null && line.length > 0) {
                        buf.append(this.format(this.transform(line), colWidths));
                    }
                }
            } else {
                String[] splitLine = this.generateSplitLine(colWidths);
                line = (String[])this.rows.get(0);
                buf.append(this.format(this.transform(splitLine), colWidths));
                buf.append(this.format(this.transform(line), colWidths));
                buf.append(this.format(this.transform(splitLine), colWidths));

                for(int r = 1; r < this.rows.size(); ++r) {
                    line = (String[])this.rows.get(r);
                    if (line != null && line.length > 0) {
                        buf.append(this.format(this.transform(line), colWidths));
                    }
                }
            }

            return buf.toString();
        }
    }

    private String[] generateSplitLine(int[] colWidths) {
        int colNum = colWidths.length;
        String[] splitLine = new String[colNum];

        for(int c = 0; c < colNum; ++c) {
            int width = colWidths[c];
            splitLine[c] = TextUtils.repeat('-', width);
        }

        return splitLine;
    }

    private String format(String[][] matrix, int[] colWidths) {
        StringBuilder sb = new StringBuilder();
        int lines = matrix.length;

        for(int i = 0; i < lines; ++i) {
            int cols = matrix[i].length;

            for(int j = 0; j < cols; ++j) {
                String s = matrix[i][j];
                int colWidth = colWidths[j];
                String formatted = TextUtils.rightPad(TextUtils.defaultString(s), colWidth);
                sb.append(formatted).append(' ');
            }

            sb.append('\n');
        }

        return sb.toString();
    }

    private String[][] transform(String[] cols) {
        for(int i = 0; i < cols.length; ++i) {
            if (cols[i] == null) {
                cols[i] = "";
            }
        }

        String ls = System.lineSeparator();
        int maxLines = 1;

        for(int c = 0; c < cols.length; ++c) {
            String col = cols[c];
            if (col.indexOf(ls) != -1) {
                int n = col.split(ls).length;
                if (n > maxLines) {
                    maxLines = n;
                }
            }
        }

        String[][] matrix = new String[maxLines][];

        int c;
        for(c = 0; c < maxLines; ++c) {
            matrix[c] = new String[cols.length];
        }

        for(c = 0; c < cols.length; ++c) {
            String col = cols[c];
            if (col.indexOf(ls) == -1) {
                matrix[0][c] = col;
            } else {
                String[] lines = col.split(ls);

                for(int r = 0; r < lines.length; ++r) {
                    matrix[r][c] = lines[r];
                }
            }
        }

        return matrix;
    }
}

