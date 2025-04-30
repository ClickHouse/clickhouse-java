package com.clickhouse.jdbc.internal;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.IterativeParseTreeWalker;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.RuleNode;
import org.testng.annotations.Test;

public class ClickhouseParserTest {


    @Test
    public void testParsingInsertAndGettingTableName() throws Exception {

        String sql = "-- comment \n" +
                "-- another comment \n" +
                "   insert INTO\n table `_some_t1` \n" +
                " (id, name, const_1) \n" +
                " /* multiline \n" +
                " comment */" +
                " VALUES (?, toString('UTF8', ?, 10), 1)";
        CharStream charStream = CharStreams.fromString(sql);
        ClickHouseLexer lexer = new ClickHouseLexer(charStream);
        ClickHouseParser parser = new ClickHouseParser(new CommonTokenStream(lexer));
        ClickHouseParser.QueryStmtContext parseTree = parser.queryStmt();
        ClickHouseParserListener parserListener = new ClickHouseParserBaseListener() {

            @Override
            public void visitErrorNode(ErrorNode node) {
                System.out.println("Error: " + node.getText());
            }

            @Override
            public void enterInsertParameterFuncExpr(ClickHouseParser.InsertParameterFuncExprContext ctx) {
                super.enterInsertParameterFuncExpr(ctx);
                System.out.println("parameter with function: " + ctx.identifier().getText());
            }

            @Override
            public void enterInsertParameter(ClickHouseParser.InsertParameterContext ctx) {
                super.enterInsertParameter(ctx);
                System.out.println("parameter: " + ctx.getText());
            }

            @Override
            public void enterInsertStmt(ClickHouseParser.InsertStmtContext ctx) {
                ClickHouseParser.TableIdentifierContext  tableId = ctx.tableIdentifier();
                if (tableId != null) {
                    String t = tableId.identifier().IDENTIFIER().getText();
                    System.out.println(tableId.getText());
                }

                ClickHouseParser.ColumnsClauseContext columns = ctx.columnsClause();
                if (columns != null) {
                    for (ClickHouseParser.NestedIdentifierContext colCtx : columns.nestedIdentifier()) {
                        System.out.println("col: " + colCtx.getText());
                    }
                }

                super.enterInsertStmt(ctx);
            }

            @Override
            public void exitInsertStmt(ClickHouseParser.InsertStmtContext ctx) {
                ClickHouseParser.TableIdentifierContext  tableId = ctx.tableIdentifier();

                super.exitInsertStmt(ctx);
            }
        };


        IterativeParseTreeWalker.DEFAULT.walk(parserListener, parseTree);
    }
}
