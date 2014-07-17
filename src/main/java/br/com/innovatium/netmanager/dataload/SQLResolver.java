package br.com.innovatium.netmanager.dataload;

import java.util.HashMap;
import java.util.Map;

final class SQLResolver {
	private Map<SQLType, String> sql = new HashMap<SQLType, String>();

	private SQLResolver(SQLResolverType type) {
		if (SQLResolverType.POSTGRE.equals(type)) {
			initPostgreSQL();
		} else if (SQLResolverType.ORACLE.equals(type)) {
			initOracleSQL();
		} else if (SQLResolverType.MYSQL.equals(type)) {
			initMySQL();
		} else {
			throw new IllegalArgumentException(
					"This database instruction was not implemented.");
		}
	}

	private void initPostgreSQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
		sql.put(SQLType.LONG_STRING, "varchar");
	}

	private void initMySQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar(4000)");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
		sql.put(SQLType.LONG_STRING, "TEXT");
	}

	private void initOracleSQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar2(4000)");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
		sql.put(SQLType.LONG_STRING, "CLOB");
	}

	public static SQLResolver getResolver(SQLResolverType type) {
		return new SQLResolver(type);
	}
	
	public String resolve(SQLType type) {
		return sql.get(type);
	}

}
