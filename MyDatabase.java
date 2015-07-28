//package file_indexing;

import java.util.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MyDatabase {
	static Scanner sc = new Scanner(System.in);
	static Map<String, List<Integer>> hm = new HashMap<String, List<Integer>>();
	static Map<String, Map<String, List<Integer>>> hm1 = new HashMap<String, Map<String, List<Integer>>>();
	
	private static byte[] asciiToHex(String asciiValue){
		char[] chars = asciiValue.toCharArray();
		byte[] byte_result = new byte[asciiValue.length()];
		int count = 0;
		for (int i = 0; i < chars.length; i++){
			int ascii = (int)chars[i];
			byte_result[count++] = (byte)ascii;
		}
		
		return byte_result;
	}
	 
	public static byte [] float2ByteArray (float value){  
	     return ByteBuffer.allocate(4).putFloat(value).array();
	}
	
	public static byte setBitForBoolean(byte in_val, boolean bool_val, int bit_pos){
		int mask;
		switch(bit_pos){
		case 1:
			mask = 1;
			break;
		case 2:
			mask = 2;
			break;
		case 3:
			mask = 4;
			break;
		case 4:
			mask = 8;
			break;
		default:
			mask = 0;
		}
		
		if(bool_val){
			in_val = (byte)(in_val | mask);
		}
		
		return in_val;
	}
	
	public static int write_binary_to_file(DataOutputStream writer, String[] field_values){
		int counter = 0;
		
		try{
			
			for(int i=0;i<7;i++){
				switch(i){
				case 0:
					int id = Integer.parseInt(field_values[i]);
					byte[] id_byte = new byte[4];
					id_byte[0] = (byte)(id>>24);
					id_byte[1] = (byte)(id>>16);
					id_byte[2] = (byte)(id>>8);
					id_byte[3] = (byte)id;
					
					writer.write(id_byte);
					counter+=4;
					break;
				case 1:
					short company_length = (short)field_values[i].length();
					writer.write((byte)company_length);
					writer.write(MyDatabase.asciiToHex(field_values[i]));
					counter+=(company_length+1);
					break;
				case 2:
					writer.write(MyDatabase.asciiToHex(field_values[2]));
					counter+=6;
					break;
				case 3:
				case 4:
				case 5:
					int val = Integer.parseInt(field_values[i]);
					byte[] val_byt = new byte[2];
					val_byt[0] = (byte)(val>>8);
					val_byt[1] = (byte)val;
					writer.write(val_byt);
					counter+=2;
					break;
				case 6:
					writer.write(MyDatabase.float2ByteArray(Float.valueOf(field_values[i])));
					counter+=4;
					break;
				}
			}
			
			int j=4;
			boolean bool_val;
			byte boolean_bits = 0x0;
			for(int i=7;i<11;i++){
				if(field_values[i].equals("true")){
					bool_val = true;
				} else {
					bool_val = false;
				}
				boolean_bits = MyDatabase.setBitForBoolean(boolean_bits, bool_val, j--);
			}
			counter+=1;
			
			writer.write(boolean_bits);
		} catch(IOException e){
			
		}
		
		return counter;
	}
	
	public static void write_hashmap(String field){
		try
        {
			FileOutputStream fos =
            new FileOutputStream(field+".ndx");
           	ObjectOutputStream oos = new ObjectOutputStream(fos);
           	oos.writeObject(hm1.get(field));
           	oos.close();
           	fos.close();
           	System.out.println("Serialized HashMap data is saved in "+field+".ndx");
        }catch(IOException ioe){
        	ioe.printStackTrace();
        }
	}
	
	public static void convert_csv2binary(){
		String csv_file;
		FileInputStream reader;
		
		try{
			DataOutputStream writer = new DataOutputStream(new FileOutputStream("data.db"));
		
			//sub-prompt for converting CSV to binary
			while(true){
				System.out.println("\nEnter the csv file path:");
				Scanner read1 = new Scanner(System.in);
				csv_file = read1.nextLine();
//				csv_file = "PHARMA_TRIALS_1000B.csv";
				
				try {
					reader = new FileInputStream(new File(csv_file));
					break;
				} catch (IOException e) {
					System.out.println("File '"+csv_file+"' not found, retry...");
				}
			}
			
			String[] fields;
			String columns;
			Scanner read_file = new Scanner(reader);
			columns = read_file.nextLine();
			fields = columns.split(",");
			
			for(String field:fields){
				hm1.put(field, new HashMap<String, List<Integer>>());
			}
			
			String str_line;
			String[] field_values;
			int starting_location = 0;
			
			int first_dq, second_dq;
			while(read_file.hasNextLine()){
				str_line = read_file.nextLine();
				if(str_line.contains("\"")){
					field_values = new String[fields.length];
					
					first_dq = str_line.indexOf('"', 0);
					second_dq = str_line.indexOf('"', first_dq+1);
					
					field_values[0] = str_line.substring(0, first_dq-1);
					field_values[1] = str_line.substring(first_dq+1, second_dq);
					String[] fld_vls = str_line.substring(second_dq+2).split(",");
					int j=2;
					for(String fld:fld_vls){
						field_values[j] = fld;
						j++;
					}
				} else {
					field_values = str_line.split(",");
				}
				
				for(int i=0;i<field_values.length;i++){
					Map<String, List<Integer>> temp = hm1.get(fields[i]);
					
					if(temp.containsKey(field_values[i])){
						List<Integer> l = temp.get(field_values[i]);
						l.add(starting_location);
					} else {
						List<Integer> l = new ArrayList<Integer>();
						l.add(starting_location);
						temp.put(field_values[i],l);
					}
				}
				
				starting_location += MyDatabase.write_binary_to_file(writer, field_values);
			}
			
			System.out.println();
			for(String field: fields){
				write_hashmap(field);
			}
			
			read_file.close();
			writer.close();
		}catch(IOException e){
			
		}
		System.out.println("binary file, indexes created successfully.");
		return;
	}
	
	public static String[] field_values_at_location(int loc){
		String[] field_values = new String[11];
		
		try{
			DataInputStream input = new DataInputStream(new FileInputStream("data.db"));
			
			if(loc>0){
				input.skipBytes(loc);
			}
			int i;
			for(i=0;i<7;i++){
				switch(i){
				case 0:
					byte[] id_byte = new byte[4];
					input.read(id_byte,0,4);
					int id;
					id = id_byte[3] & 0xFF |
				            (id_byte[2] & 0xFF) << 8 |
				            (id_byte[1] & 0xFF) << 16 |
				            (id_byte[0] & 0xFF) << 24;
					field_values[i] = Integer.toString(id);
					break;
				case 1:
					byte[] byt = new byte[1];
					input.read(byt,0,1);
					int len = byt[0] & 0xFF;
					byte[] company = new byte[len];
					input.read(company,0,len);
					String decoded = new String(company, "ASCII");
					field_values[i] = decoded;
					break;
				case 2:
					byte[] drug_id = new byte[6];
					input.read(drug_id,0,6);
					String drug = new String(drug_id, "ASCII");
					field_values[i] = drug;
					break;
				case 3:
				case 4:
				case 5:
					byte[] two_byt = new byte[2];
					input.read(two_byt,0,2);
					int short_int;
					short_int = (two_byt[1] & 0xFF) | (two_byt[0] & 0xFF) << 8;
					field_values[i] = Integer.toString(short_int);
					break;
				case 6:
					byte[] float_byt = new byte[4];
					input.read(float_byt,0,4);
					ByteBuffer buffer = ByteBuffer.wrap(float_byt);

					float second = buffer.getFloat();
					field_values[i] = Float.toString(second);
				}
			}
			
			byte[] bool_byt = new byte[1];
			input.read(bool_byt,0,1);
			field_values[i++] = (((bool_byt[0] & 0xFFFF) & 8)>0)?"true":"false";
			field_values[i++] = (((bool_byt[0] & 0xFFFF) & 4)>0)?"true":"false";
			field_values[i++] = (((bool_byt[0] & 0xFFFF) & 2)>0)?"true":"false";
			field_values[i++] = (((bool_byt[0] & 0xFFFF) & 1)>0)?"true":"false";
			
			input.close();
		}catch(IOException e){
			System.out.println("something is wrong");
		}
		return field_values;
	}
	
	@SuppressWarnings("unchecked")
	public static Map<String, List<Integer>> read_index(String field){
		Map<String, List<Integer>> index_map = null;
		try
        {
			FileInputStream fis = new FileInputStream(field+".ndx");
           	ObjectInputStream ois = new ObjectInputStream(fis);
           	index_map = (Map<String, List<Integer>>)ois.readObject();
           	fis.close();
           	ois.close();
        }catch(IOException ioe){
        	ioe.printStackTrace();
        }catch(ClassNotFoundException e){
        	
        }
		return index_map;
	}
	
	public static void query_field(){
		int i=1;
		Map<Integer, String> fields = new HashMap<Integer, String>();
		fields.put(i++, "id");
		fields.put(i++, "company");
		fields.put(i++, "drug_id");
		fields.put(i++, "trials");
		fields.put(i++, "patients");
		fields.put(i++, "dosage_mg");
		fields.put(i++, "reading");
		fields.put(i++, "double_blind");
		fields.put(i++, "controlled_study");
		fields.put(i++, "govt_funded");
		fields.put(i++, "fda_approved");
		
		for(int j=1;j<=11;j++){
			try{
				FileReader fr = new FileReader(fields.get(j)+".ndx");
				fr.close();
			}catch(FileNotFoundException e){
				System.out.println("Index files missing. Create binary files first");
				return;
			}catch(IOException e){
				
			}
		}
		
		try{
			FileReader fr = new FileReader("data.db");
			fr.close();
		}catch(FileNotFoundException e){
			System.out.println("Binary file missing. Create binary files first");
			return;
		}catch(IOException e){
			
		}
		
		// prompt for querying field by values
		System.out.println();
		System.out.println("Choose from any one of the fields below");
		
		for(int j=1;j<=11;j++){
			System.out.println(j + ". " + fields.get(j));
		}
		
		int field_index;
		while(true){
			try {
				System.out.print("Enter your option:");
				field_index = MyDatabase.sc.nextInt();
				
				if(field_index >=1 && field_index <= 11){
					break;
				} else {
					System.out.println("Invalid option");
				}
			} catch(InputMismatchException e){
				System.out.println("Invalid input string");
				sc.next();
			}
		}
		
		Map<Integer, String> operation_hash = new HashMap<Integer, String>();
		operation_hash.put(1, "=");
		operation_hash.put(2, ">");
		operation_hash.put(3, "<");
		operation_hash.put(4, ">=");
		operation_hash.put(5, "<=");
		operation_hash.put(6, "!=");
		
		System.out.println();
		System.out.println("Chose the comparison operation");
		int operation;
		if(field_index>=1 && field_index<=7){
			for(int j=1;j<=6;j++){
				System.out.println(j + ". " + operation_hash.get(j));
			}
		
			while(true){
				try {
					System.out.print("Enter your option:");
					operation = MyDatabase.sc.nextInt();
					
					if(operation >=1 && operation <= 6){
						break;
					} else {
						System.out.println("Invalid operation");
					}
				} catch(InputMismatchException e){
					System.out.println("Invalid input string");
					sc.next();
				}
			}
		} else {
			operation = 1;
		}
		
		String field_value;
		if(field_index >=8 && field_index <= 11){
			System.out.println();
			System.out.println("Enter the value to check for:");
			System.out.println("1. True");
			System.out.println("2. False");
			
			int t_or_f;
			while(true){
				try {
					System.out.print("Enter your option:");
					t_or_f = MyDatabase.sc.nextInt();
					
					if(t_or_f >=1 && t_or_f <= 2){
						break;
					} else {
						System.out.println("Invalid option");
					}
				} catch(InputMismatchException e){
					System.out.println("Invalid input string");
					sc.next();
				}
			}
			
			if(t_or_f == 1){
				field_value = "true"; 
			} else {
				field_value = "false";
			}
		} else {
			System.out.println();
			System.out.println("Enter the field value:");
			Scanner scn = new Scanner(System.in);
			field_value = scn.nextLine();
			
		}
		
		String field_chosen = fields.get(field_index);
		
		System.out.println();
		System.out.println("Querying for '"+field_chosen+"' "+operation_hash.get(operation)
					+" "+field_value);
		
		boolean printed_records = false;
		
		List<Integer> lst = null;
		Map<String, List<Integer>> key_values = read_index(field_chosen);
		Set<String> keys = key_values.keySet();
		Iterator<String> key_itr;
		switch(operation){
		case 1:
			switch(field_index){
			case 1: case 2: case 3: case 4: case 5: case 6:
				if(keys.contains(field_value)){
					lst = key_values.get(field_value);
					print_records(lst);
					printed_records = true;
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) == Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8: case 9: case 10: case 11:
				if(field_value.toLowerCase().equals("true")){
					field_value = "true";
				} else {
					field_value = "false";
				}
				if(keys.contains(field_value)){
					lst = key_values.get(field_value);
					print_records(lst);
					printed_records = true;
				}
				break;
			}
			break;
		case 2:
			switch(field_index){
			case 1: case 4: case 5: case 6:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Integer.parseInt(vl) > Integer.parseInt(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 2: case 3:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(vl.compareTo(field_value) > 0){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) > Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8:case 9: case 10: case 11:
				System.out.println("invalid operation");
			}
			break;
		case 3:
			switch(field_index){
			case 1: case 4: case 5: case 6:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Integer.parseInt(vl) < Integer.parseInt(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 2: case 3:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(vl.compareTo(field_value) < 0){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) < Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8:case 9: case 10: case 11:
				System.out.println("invalid operation");
			}
			break;
		case 4:
			switch(field_index){
			case 1: case 4: case 5: case 6:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Integer.parseInt(vl) >= Integer.parseInt(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 2: case 3:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(vl.compareTo(field_value) >= 0){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) >= Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8:case 9: case 10: case 11:
				System.out.println("invalid operation");
			}
			break;
		case 5:
			switch(field_index){
			case 1: case 4: case 5: case 6:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Integer.parseInt(vl) <= Integer.parseInt(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 2: case 3:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(vl.compareTo(field_value) <= 0){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) <= Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8:case 9: case 10: case 11:
				System.out.println("invalid operation");
			}
			break;
		case 6:
			switch(field_index){
			case 1: case 4: case 5: case 6:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Integer.parseInt(vl) != Integer.parseInt(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 2: case 3:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(!vl.equals(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 7:
				key_itr = keys.iterator();
				while(key_itr.hasNext()){
					String vl = key_itr.next();
					if(Float.parseFloat(vl) != Float.parseFloat(field_value)){
						lst = key_values.get(vl);
						print_records(lst);
						printed_records = true;
					}
				}
				break;
			case 8:case 9: case 10: case 11:
				System.out.println("invalid operation");
			}
			break;
		}
		
		if(!printed_records){
			System.out.println("Matching records NOT found");
		}
		
		System.out.println();
		System.out.println("exiting querying");
		return;
	}
	
	public static void print_records(List<Integer> lst){
		Iterator<Integer> itr = lst.iterator();
		while(itr.hasNext()){
			String[] field_values = MyDatabase.field_values_at_location((int)itr.next());
			for(String field:field_values){
				System.out.print(field+" ");
			}
			System.out.println();
		}
	}
	
	public static void display_prompt_screen(){
		int option;
		boolean loop = true;
		
		while(loop){
			option = 3;
			System.out.println();
			System.out.println("Enter option:");
			System.out.println("1. Convert CSV to binary file");
			System.out.println("2. Query based on fields");
			System.out.println("3. Exit program.");
			
			while(true){
				try {
					System.out.print("Enter your option:");
					option = MyDatabase.sc.nextInt();
					break;
				} catch(InputMismatchException e){
					System.out.println("Invalid input string");
					sc.next();
				}
			}
			
			switch(option){
			case 1:
				convert_csv2binary();
				break;
			case 2:
				query_field();
				break;
			case 3:
				loop = false;
				break;
			default:
				System.out.println("Invalid option selected!");
			}
		}
		
	}
	
	public static void main(String[] args){
		System.out.println("Program started ...");
		
		// prompt
		display_prompt_screen();
		
		System.out.println("\nProgram completed.");
	}
}