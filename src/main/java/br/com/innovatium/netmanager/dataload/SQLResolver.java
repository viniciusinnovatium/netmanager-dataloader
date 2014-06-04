package br.com.innovatium.netmanager.dataload;

import java.util.HashMap;
import java.util.Map;

final class SQLResolver {
	private Map<SQLType, String> sql = new HashMap<SQLType, String>();

	private SQLResolver(ResolverType type) {
		if (ResolverType.POSTGRE.equals(type)) {
			initPostgreSQL();
		} else if (ResolverType.ORACLE.equals(type)) {
			initOracleSQL();
		} else if (ResolverType.MYSQL.equals(type)) {
			initPostgreMySQL();
		} else {
			throw new IllegalArgumentException(
					"This database instruction was not implemented.");
		}
	}

	private void initPostgreSQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar(4000)");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
	}

	private void initPostgreMySQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar(4000)");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
	}

	private void initOracleSQL() {
		sql = new HashMap<SQLType, String>();
		sql.put(SQLType.STRING, "varchar2(4000)");
		sql.put(SQLType.DROP_TABLE, "drop table");
		sql.put(SQLType.CREATE_TABLE, "create table");
	}

	public static SQLResolver getResolver(ResolverType type) {
		return new SQLResolver(type);
	}

	public String resolve(SQLType type) {
		return sql.get(type);
	}

}
