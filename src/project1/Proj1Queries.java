package project1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class Proj1Queries {

	public static boolean createDepartmentTable(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "create table if not exists departments ( "+
					       		"deptId INT AUTO_INCREMENT, "+
					       		"deptAbbrev char(8) NOT NULL, "+
					       		"deptName varchar(50) NOT NULL, "+
					       		"primary key(deptId) "+
                           ")ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.executeUpdate();
				
				success=true;
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean createEmployeeTable(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "CREATE TABLE if not exists employees ( "+
								"empId int(11) NOT NULL AUTO_INCREMENT, "+
								"empName varchar(150) NOT NULL, "+
								"deptId int(11) DEFAULT NULL,"+ 
								"salary int(11) DEFAULT NULL, "+
								"status varchar(50) DEFAULT 'Active', "+
								"education varchar(50) DEFAULT NULL, "+
								"PRIMARY KEY (empId), "+
								"KEY FK_deptId (deptId), "+
								"CONSTRAINT FK_deptId FOREIGN KEY (deptId) " +
								 	"REFERENCES departments (deptId)  ON DELETE SET NULL "+
							") ENGINE=InnoDB DEFAULT CHARSET=latin1; ";
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.executeUpdate();
				
				success=true;
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean insertEmployees(Connection conn){
		boolean success = false;		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "INSERT INTO `employees` (`empName`, `deptId`, `salary`, `education`) "+
						   "VALUES "+
						   		"('Ann Lee',1, 3500, 'Certification'), "+
						   		"('Ryan Gonzalez',1, 4000, 'BA'), "+
						   		"('Wanda Delgado',4, 5000, 'BA'), "+
						   		"('Kate Sharp',12, 5000, 'MA'), "+
						   		"('Rafael Flores',1, 10000, 'PhD'), "+
						   		"('Gilbert Mendoza',5, 9000, 'MA'), "+
						   		"('Ronald Obrien',5, 4500, 'AA'), "+
						   		"('Alvin Miller',2, 4870, 'AA'), "+
						   		"('Eugene Ruiz',1, 11000, 'BA'), "+
						   		"('Phillip Adkins',3, 8000,'BA'), "+
						   		"('Arnetta Mccain',15, 5000, 'BA'), "+
						   		"('Johanne Tobin',14, 4000, 'AA'), "+
						   		"('Trent Rohr',13, 4600, 'Certification'), "+
						   		"('Dick Whitman',6, 12500, 'BA'), "+
						   		"('Ronni Dwyer',7, 3400, 'PhD'), "+
						   		"('Doreatha Lugo',8, 3000, 'AA'), "+
						   		"('Gwenn Cheung',9, 9500, 'Certification'); ";
			try {
				stmt = conn.prepareStatement(query);
				int count = stmt.executeUpdate();
				System.out.println(count + " rows updated");
				if(count>0){
					success=true;
				}
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}

	public static boolean insertDepartments(Connection conn){
		boolean success = false;
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "INSERT INTO `departments` (`deptAbbrev`, `deptName`) "+
						   "VALUES "+
						    	"('ACCT','Acountant'), "+
								"('IT','Technology'), "+
								"('HR','Human Reources'), "+
								"('TRANS','Transportation'), "+
								"('ENGR','Engineering'), "+
								"('MARK','Marketing'), "+
								"('FOOD','Food services'), "+
								"('SALE','Sales'), "+
								"('FI','Finance'), "+
								"('ADS','Advertisment'), "+
								"('SUPP','Supports'), "+
								"('DELI','Delivery'), "+
								"('QA','Tesings'), "+
								"('JOUR','Journalism'), "+
								"('HEALTH','Health management'), "+
								"('PR','Public Relation'); ";
			
			try {
				stmt = conn.prepareStatement(query);
				int count = stmt.executeUpdate();
				System.out.println(count + " rows updated");
				if(count>0){
					success=true;
				}
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		return success;
	}
	
	public static boolean showDepartments(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select * from departments; ";
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\ndeptID | deptAbbrev | deptName\n");
				while(rs.next()){
					int deptId = rs.getInt("deptId");
					String deptAbbrev = rs.getString("deptAbbrev");
					String deptName = rs.getString("deptName");
					System.out.println(deptId+" | "+deptAbbrev+" | "+deptName);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean showEmployees(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select * from employees; ";
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | deptId | salary | status | education\n");
				while(rs.next()){
					int empId = rs.getInt("empId");
					int deptId = rs.getInt("deptId");
					String empName = rs.getString("empName");
					String salary = rs.getString("salary");
					String status = rs.getString("status");
					String education = rs.getString("education");
					System.out.println(empId+" | "+empName+" | "+deptId+" | "+salary+" | "+status+" | "+education);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		return success;
	}
	
	public static boolean sortEmployeesByName(Connection conn, String dir){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select * from employees order by empName " + dir ;
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | deptId | salary | status | education\n");
				while(rs.next()){
					int empId = rs.getInt("empId");
					int deptId = rs.getInt("deptId");
					String empName = rs.getString("empName");
					String salary = rs.getString("salary");
					String status = rs.getString("status");
					String education = rs.getString("education");
					System.out.println(empId+" | "+empName+" | "+deptId+" | "+salary+" | "+status+" | "+education);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean findEmpByDeptAndSalary(Connection conn, int salary, String deptAbbr){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select * from employees, departments "+
						   "where employees.deptId = departments.deptId " +
						     	  "and employees.salary >= ? and deptAbbrev = ?; ";
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, salary);
				stmt.setString(2, deptAbbr);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | deptId | salary | status | education\n");
				while(rs.next()){
					int empId = rs.getInt("empId");
					int deptId = rs.getInt("deptId");
					String empName = rs.getString("empName");
					String sa = rs.getString("salary");
					String status = rs.getString("status");
					String education = rs.getString("education");
					System.out.println(empId+" | "+empName+" | "+deptId+" | "+sa+" | "+status+" | "+education);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean DepartmentsJoinEmployees(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select employees.empId, employees.empName, employees.salary, " +
					 			  "departments.deptAbbrev,  departments.deptName "+
						   "from employees, departments "+
						   "where employees.deptId=departments.deptId; ";
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | salary | deptAbbre | deptName\n");
				while(rs.next()){
					int empId = rs.getInt("empId");
					String empName = rs.getString("empName");
					String salary = rs.getString("salary");
					String deptAbbrev = rs.getString("deptAbbrev");
					String deptName = rs.getString("deptName");
					System.out.println(empId+" | "+empName+" | "+salary+" | "+deptAbbrev+" | "+deptName);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
		
	public static boolean TotalEmployeesinDepartments(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select departments.deptAbbrev, departments.deptName, empCount.totalEmployees "+
						   "from departments, "+ 
						   		 "(select departments.`deptId`, count(empId) as totalEmployees "+
						   		  "from employees, departments where employees.deptId=departments.deptId "+
						   		  "group by departments.deptId "+
						   		  "order by departments.deptName asc) empCount "+
						   "where departments.deptId=empCount.deptId; ";
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\ndeptAbbre | deptName | totalEmployees\n");
				while(rs.next()){
			
					String deptAbbrev = rs.getString("deptAbbrev");
					String deptName = rs.getString("deptName");
					int totalEmployees = rs.getInt("totalEmployees");
					System.out.println(deptAbbrev+" | "+deptName+" | "+totalEmployees);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean top5HighestPaid(Connection conn){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select employees.empName, employees.salary "+
						   "from employees "+
						   "ORDER BY employees.salary desc "+
						   "limit 5; ";
			
			try {
				stmt = conn.prepareStatement(query);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempName | salary\n");
				while(rs.next()){
			
					String empName = rs.getString("empName");

					int salary = rs.getInt("salary");
					System.out.println(empName+" | "+salary);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean showEmployeeById(Connection conn, int empId){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select * "+ 
						   "from employees "+
						   "where employees.`empId`= ?; ";
			
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, empId);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | salary | status | education | deptId \n");
				while(rs.next()){
			
				
					int eId = rs.getInt("empId");
					int salary = rs.getInt("salary");

					String deptId = rs.getString("deptId");
					
					String empName = rs.getString("empName");
					String status = rs.getString("status");
					String education = rs.getString("education");
					System.out.println(eId+" | "+empName+" | "+salary+" | "+status+" | "+education+" | "+deptId);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean showEmployeeandDeptById(Connection conn, int empId){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "select employees.`empId`, employees.`empName`, employees.`salary`, " +
								   "employees.`status`, employees.`education`, departments.`deptAbbrev`, "+
								   "departments.`deptName` "+ 
						   "from employees, departments "+
						   "where  employees.`deptId` = departments.`deptId` and employees.`empId`= ?; ";
			
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, empId);
				ResultSet rs = stmt.executeQuery();
				
				System.out.println("\nempId | empName | salary | status | education | deptAbbrev | deptName\n");
				while(rs.next()){
			
					String deptAbbrev = rs.getString("deptAbbrev");
					String deptName = rs.getString("deptName");
					int eId = rs.getInt("empId");
					int salary = rs.getInt("salary");
					String empName = rs.getString("empName");
					String status = rs.getString("status");
					String education = rs.getString("education");
					System.out.println(eId+" | "+empName+" | "+salary+" | "+status+" | "+education+" | "+deptAbbrev+" | "+deptName);
					
				}
				
				success = true;
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean deleteDepartment(Connection conn, int deptId){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "delete from departments "+
						   "where deptId=?; ";
			
			
			try {
				stmt = conn.prepareStatement(query);
		
				stmt.setInt(1, deptId);
				int count = stmt.executeUpdate();
				
				success = true;
				System.out.println(count+" rows affected");
				
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
	public static boolean updateDepartment(Connection conn, String abbr, String deptName, int deptId){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "update departments "+
						   "set deptAbbrev = ?, deptName= ? "+
						   "where deptId=?; ";
			
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.setString(1, abbr);
				stmt.setString(2, deptName);
				stmt.setInt(3, deptId);
				int count = stmt.executeUpdate();
				
				success = true;
				System.out.println(count+" rows affected");
				
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}

	public static boolean updateSetInActive(Connection conn,  int empId){
		boolean success = false;
		
		if(conn!=null){
			PreparedStatement stmt = null;
			String query = "update employees "+
						   "set status = 'Inactive', deptId= NULL "+
						   "where empId=?; ";
			
			
			try {
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, empId);
				
				int count = stmt.executeUpdate();
				
				success = true;
				System.out.println(count+" rows affected");
				
				
				if(stmt!=null){
					stmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				success=false;
			} finally{
				Connections.disConnectDB(conn);
			}
			
		}else {
			success = false;
		}
		
		return success;
	}
	
}
