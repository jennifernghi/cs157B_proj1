package project1;


public class ProjectRunner {
	
	

	public static void main(String[] args) {
		
		if(1==Connections.setUpDriver()){ 
			System.out.println("-------------Creating departments tables-------------");
			if(!Proj1Queries.createDepartmentTable(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD))){
				System.out.println("table departments creation failed");
			}else{
				
				System.out.println("table departments created");
			}
			
			System.out.println("\n-------------Creating employees tables-------------");

			if(!Proj1Queries.createEmployeeTable(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD))){
				System.out.println("table employees creation failed");
			}else{
				
				System.out.println("table employees created");
			}
			
			System.out.println("\n-------------Insert into departments tables-------------");

			if(!Proj1Queries.insertDepartments(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD))){
				System.out.println("Insertion failed");
			}else{
				Proj1Queries.showDepartments(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
			
				System.out.println("\n-------------Insert into employees tables-------------");
				
				if(!Proj1Queries.insertEmployees(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD))){
					System.out.println("Insertion failed");
				}else{
					
					Proj1Queries.showEmployees(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
				
				
					System.out.println("\n-------------Join: Employee and department-------------");
					Proj1Queries.DepartmentsJoinEmployees(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
					
					System.out.println("\n-------------find how many employees in each department-------------");
					
					Proj1Queries.TotalEmployeesinDepartments(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
					System.out.println("\n-------------find top 5 highest paid employees-------------");
					
					Proj1Queries.top5HighestPaid(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
					
					
					System.out.println("\n-------------Sort employee by name-------------");
					Proj1Queries.sortEmployeesByName(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), "asc");
					
					
					System.out.println("\n-------------Find employees who earn more than 8000 in ACCT department-------------");
					Proj1Queries.findEmpByDeptAndSalary(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 8000, "ACCT");
					
				
					System.out.println("\n-------------Delete 'Public Relation' depId=16 off department-------------");
					System.out.println("\nBefore-----");
					Proj1Queries.showDepartments(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
				
					System.out.println("\nAfter-----");
					//Delete 'Public Relation' depId=16 
					Proj1Queries.deleteDepartment(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 16);
					Proj1Queries.showDepartments(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD));
					
					System.out.println("\n-------------Set inactive status to 1 employee-------------");
					System.out.println("\nBefore-----");
					Proj1Queries.showEmployeeById(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 10);
					System.out.println("\nAfter-----");
					Proj1Queries.updateSetInActive(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 10);
					Proj1Queries.showEmployeeById(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 10);
		
					System.out.println("\n-------------Update a department Accountant to Product Management-------------");
					System.out.println("\nBefore-----");
					Proj1Queries.showEmployeeandDeptById(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 1);
					System.out.println("\nAfter-----");
					//update Accountant to Product Management
					Proj1Queries.updateDepartment(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), "PROD", "Product Management", 1);
					Proj1Queries.showEmployeeandDeptById(Connections.connectDB(DBUtils.DB_URL, DBUtils.DB_USER, DBUtils.DB_PASSWORD), 1);
					
				}
			}
				
		}else{
			 System.out.println("JDBC set up failed!");
		}
		
		
	}
	
	

}
