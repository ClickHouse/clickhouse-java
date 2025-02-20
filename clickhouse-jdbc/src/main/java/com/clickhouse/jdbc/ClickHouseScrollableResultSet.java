package com.clickhouse.jdbc;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.client.ClickHouseResponse;

@Deprecated
public class ClickHouseScrollableResultSet extends ClickHouseResultSet {

	private final List<byte[]> records;

	public ClickHouseScrollableResultSet(String database, String table, ClickHouseStatement statement,
			ClickHouseResponse response) throws SQLException {
		super(database, table, statement, response);

		this.records = new LinkedList<>();
	}

	@Override
	public int getType() throws SQLException {
		return TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public void beforeFirst() throws SQLException {
		absolute(0);
	}

	@Override
	public void afterLast() throws SQLException {
		absolute(-1);
		next();
	}

	@Override
	public boolean first() throws SQLException {
		return absolute(1);
	}

	@Override
	public boolean last() throws SQLException {
		return absolute(-1);
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		return false;
		// TODO implemetation
		/*
		 * if (row == 0) { rowNumber = 0; values = null; return false; } else if (row >
		 * 0) { if (row <= lines.size()) { rowNumber = row; values = lines.get(row - 1);
		 * return true; } absolute(lines.size()); while (getRow() < row && hasNext()) {
		 * next(); } if (row == getRow()) { return true; } else { next(); return false;
		 * } } else { // We have to check the number of total rows while (hasNext()) {
		 * next(); } if (-row > lines.size()) { // there is not so many rows // Put the
		 * cursor before the first row return absolute(0); } return
		 * absolute(lines.size() + 1 + row); }
		 */
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		int r = getRow() + rows;
		if (r < 0) {
			r = 0;
		}
		return absolute(r);
	}

	@Override
	public boolean previous() throws SQLException {
		return relative(-1);
	}
}
