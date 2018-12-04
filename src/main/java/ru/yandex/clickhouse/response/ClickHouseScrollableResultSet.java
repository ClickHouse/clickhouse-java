package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;


public class ClickHouseScrollableResultSet extends ClickHouseResultSet {
    
    private List<ByteFragment[]> lines;

    public ClickHouseScrollableResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals, ClickHouseStatement statement, TimeZone timezone, ClickHouseProperties properties) throws IOException {
        super(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
        lines = new ArrayList<ByteFragment[]>();
    }

    public boolean hasNext() throws SQLException {
    	if(rowNumber < lines.size()) {
    		return true;
    	}
        return super.hasNext();
    }

    @Override
    public boolean next() throws SQLException {
    	if(rowNumber < lines.size()) {
    		values = lines.get(rowNumber);
    		nextLine = null;
            rowNumber += 1;
    		return true;
    	}
        if (hasNext()) {
            super.next();
            lines.add(values);
            return true;
        } else {
        	rowNumber += 1;
        	values = null;
        	nextLine = null;
        	return false;
        }
    }

    //////

    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return getRow() == 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return getRow() > lines.size();
	}

	@Override
	public boolean isFirst() throws SQLException {
		return getRow() == 1;
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
		if(row == 0) {
			rowNumber = 0;
			values = null;
			return true;
		} else if(row > 0) {
			if(row <= lines.size()) {
				rowNumber = row;
				values = lines.get(row-1);
				return true;
			}
			absolute(lines.size());
			while(getRow() < row && hasNext()) {
				next();
			}
			return row == getRow();
		} else {
			int current = rowNumber;
			// We have to check the number of total rows
			while(hasNext()) {
				next();
			}
			if(-row > lines.size()) {
				// there is not so many rows
				// Put back the cursor where it was.
				absolute(current);
				return false;
			}
			return absolute(lines.size()+1+row);
		}
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return absolute(getRow()+rows);
	}

	@Override
	public boolean previous() throws SQLException {
		return relative(-1);
	}
}
