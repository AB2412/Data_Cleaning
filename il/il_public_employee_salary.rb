# Creator:      Sergii Butrymenko
# Dataset Name: Illinois Public Employee Salary
# Task #:       115
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/573
# Dataset Link: https://lokic.locallabs.com/data_sets/70
# Created:      May 2023

# ruby mlc.rb --tool="clean::il::il_public_employee_salary"
# ruby mlc.rb --tool="clean::il::il_public_employee_salary" --mode='agency'
# ruby mlc.rb --tool="clean::il::il_public_employee_salary" --mode='name'
# ruby mlc.rb --tool="clean::il::il_public_employee_salary" --mode='position'
# ruby mlc.rb --tool="clean::il::il_public_employee_salary" --mode='office'

def execute(options = {})
  route = C::Mysql.on(DB01, 'il_raw')
  table_description = {
    agency: {
      raw_table: 'il_gov_employee_salaries',
      clean_table: 'il_gov_employee_salaries__agencies_clean',
      raw_column: 'agency',
      clean_column: 'agency_clean',
    },
    name: {
      raw_table: 'il_gov_employee_salaries',
      clean_table: 'il_gov_employee_salaries__names_clean',
      raw_column: 'full_name',
      clean_column: 'full_name_clean',
    },
    position: {
      raw_table: 'il_gov_employee_salaries',
      clean_table: 'il_gov_employee_salaries__positions_clean',
      raw_column: 'position',
      clean_column: 'position_clean',
    },
    office: {
      raw_table: 'il_gov_employee_salaries__locations_matched',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :agency
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_agencies(table_info, route)
  when :name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_names(table_info, route)
  when :position
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_titles(table_info, route)
  when :office
    clean_cities_and_zips(route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #115] DS #467 Illinois Public Employee Salary* \n>#{type} #{message}",
    as_user: true
  )
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(2),"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
         # CHARACTER SET latin1 COLLATE latin1_swedish_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.id IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  return if names_list.empty?

  parts = names_list.each_slice(10_000).to_a
  parts.each do |part|
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}'),"
    end
    insert_query = "#{insert_query.chop};"
    puts insert_query.red
    route.query(insert_query)
  end
end

def clean_agencies(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  universities_list = route.query(query).to_a
  return if universities_list.empty?

  universities_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(clean_name[table_info[:raw_column]].dup)
    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query
    route.query(update_query)
  end
end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  names_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    result_name = row[table_info[:raw_column]].dup
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name)
    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query
    route.query(update_query)
  end
end

def clean_titles(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  titles_list = route.query(query).to_a

  titles_list.each do |item|
    puts JSON.pretty_generate(item).yellow
    clean_title = item[table_info[:raw_column]].dup.gsub(/\.(?!(\s|,|$))/, '. ').sub(/,?\sand\sa$/i, '')

    # clean_title = clean_title.gsub(/([[:lower:]\d])([[:upper:]])/, '\1 \2').gsub(/([^-\d])(\d[-\d]*( |$))/,'\1 \2').gsub(/([[:upper:]])([[:upper:]][[:lower:]\d])/, '\1 \2').gsub(/(?<!\s)&/, ' &').gsub(/&(?!\s)/, '& ')

    clean_title.gsub!(/\bACCT FISCAL ADMIN CAREER TRAINEE\b/i, 'Accounting and Fiscal Administration Career Trainee')
    clean_title.gsub!(/\bACCT PAYABLE CLERK\/TYPIST\b/i, 'Accounts Payable Clerk/Typist')
    clean_title.gsub!(/\bACP DIRECTOR\b/i, 'Address Confidentiality Program Director')
    clean_title.gsub!(/\bACTING CHIEF ENGINEERING OFFICER\b/i, 'Acting Chief of Engineering Officer')
    clean_title.gsub!(/\bACTING CHIEF INTERNAL AUDITOR\b/i, 'Acting Chief of Internal Auditor')
    clean_title.gsub!(/\bADM ASST TO SUPREME COURT JUSTIC\b/i, 'Administrative Assistant to Supreme Court Justice')
    clean_title.gsub!(/\bADMIN ASSIST TO ASSIST DIRECTOR\b/i, 'Administrative Assistant to Assistant Director')
    clean_title.gsub!(/\bADMIN ASSISTANT 2\/RECEPTIONIST\b/i, 'Administrative Assistant 2/Receptionist')
    clean_title.gsub!(/\bADMIN ASSISTANT I\b/i, 'Administrative Assistant 1')
    clean_title.gsub!(/\bADMIN ASST II\b/i, 'Administrative Assistant 2')
    clean_title.gsub!(/\bADMIN ASST MINORITY LEADER HOUS\b/i, 'Administrative Assistant Minority Leader House')
    clean_title.gsub!(/\bADMIN ASST TO THE MARSHAL\b/i, 'Administrative Assistant to the Marshal')
    clean_title.gsub!(/\bADMIN LAW JUDGE TRANS IV\b/i, 'Administrative Law Judge Transportation 4')
    clean_title.gsub!(/\bADMN AIDE\b/i, 'Administrative Aide')
    clean_title.gsub!(/\bAG LAND & WATER RESRCE SPEC III\b/i, 'Agricultural Land & Water Resource Specialist 3')
    clean_title.gsub!(/\bAG LAND&WATER RESOURCES SUPERVIS\b/i, 'Agricultural Land & Water Resources Supervisor')
    clean_title.gsub!(/\bAGRICULTURAL MARKETING REPRESENT\b/i, 'Agricultural Marketing Representative')
    clean_title.gsub!(/\bANIMAL AND ANIMAL PROD INVEST\b/i, 'Animal & Animal Products Investigator')
    clean_title.gsub!(/\bANIMAL&ANIMAL PROD INVEST TRAIN\b/i, 'Animal & Animal Products Investigator Trainee')
    clean_title.gsub!(/\bAPPEL CRT LEGAL RESEARCH AST DIR\b/i, 'Appellate Court Legal Research Assistant Director')
    clean_title.gsub!(/\bAPPELL COURT JUDICIAL SECRETARY\b/i, 'Appellate Court Judicial Secretary')
    clean_title.gsub!(/\bAPPELL COURT LEGAL RESEARCH DIR\b/i, 'Appellate Court Legal Research Director')
    clean_title.gsub!(/\bAPPROP MGR FOR POLICY AND BUDGET\b/i, 'Appropriations Manager for Policy and Budget')
    clean_title.gsub!(/\bASSIST DIR DEPT OF CORRECTIONS\b/i, 'Assistant Director Department of Corrections')
    clean_title.gsub!(/\bASSISTANT DOORKEEPER HOUSE\b/i, 'Assistant Doorkeeper of the House')
    clean_title.gsub!(/\bASSOC COM DIR & CHF OF SPCHWRTNG\b/i, 'Associate Communications Director & Chief of Speechwriting')
    clean_title.gsub!(/\bASSOC GEN COUNCIL & POLICY ADV\b/i, 'Associate General Council & Policy Advisor')
    clean_title.gsub!(/\bASSOC GEN COUNCIL & POLICY ADV\b/i, 'Associate General Council & Policy Advisor')
    clean_title.gsub!(/\bASST ADJ GENERAL-ARMY\b/i, 'Assistant Adjutant General-Army')
    clean_title.gsub!(/\bASST COUNTY SUPT OF SCHOOLS\b/i, 'Assistant County Superintendent of Schools')
    clean_title.gsub!(/\bASST DIR COMMERCE & ECONOMIC OPP\b/i, 'Assistant Director of Commerce & Economic Opportunity')
    clean_title.gsub!(/\bASST DIR HEALTHCARE&FAMILY SERV\b/i, 'Assistant Director Healthcare & Family Services')
    clean_title.gsub!(/\bASST DIR OF REV\b/i, 'Assistant Director of Revenue')
    clean_title.gsub!(/\bASST SUPV-ACCTS PAYABLE\b/i, 'Assistant Supervisor-Accounts Payable')
    clean_title.gsub!(/\bAUTOMOBILE ADMIN & MAINTENANCE\b/i, 'Automobile Administration & Maintenance')
    clean_title.gsub!(/\bCAP DEV BD ART IN ARCHITEC TECH\b/i, 'Capital Development Board Art-in-Architecture Technician')
    clean_title.gsub!(/\bCAP POLICE INVEST-LIEUTENANT\b/i, 'Capitol Police Investigator - Lieutenant')
    clean_title.gsub!(/\bCASHIER\/GENERAL ACCNT ASST\b/i, 'Cashier General Accounting Assistant')
    clean_title.gsub!(/\bCH DIVERSITY & INCLUSION OFFICER\b/i, 'Chief Diversity & Inclusion Officer')
    clean_title.gsub!(/\bCHAIR OF CIVIL SERV COMMISSION\b/i, 'Chairman of Civil Services Commission')
    clean_title.gsub!(/\bCHAIRMAN-IL WORKERS COMP COMM\b/i, 'Chairman-Illinois Workers Compensation Commission')
    clean_title.gsub!(/\bCHF OPERATING OFF & CHF FIN OFC\b/i, 'Chief Operating Officer & Chief Financial Officer')
    clean_title.gsub!(/\bCHF,MEDICAID FRAUD,INVESTIGATION\b/i, 'Chief, Medicaid Fraud, Investigation')
    clean_title.gsub!(/\bCHIEF ADMIN OFFICER\/SR COUNSEL\b/i, 'Chief Administrative Officer/Senior Counsel')
    clean_title.gsub!(/\bCHIEF FINANCIAL OFCR\/DEI COUNSEL\b/i, 'Chief Financial Officer & Diversity, Equity and Inclusion Counsel')
    clean_title.gsub!(/\bCHIEF INFORMATION SECURITY OFC\b/i, 'Chief Information Security Officer')
    clean_title.gsub!(/\bCHIEF VET TECHNICIAN\b/i, 'Chief Veterinarian Technician')
    clean_title.gsub!(/\bCHILD WELFARE ADM CASE REVIEWER\b/i, 'Child Welfare Administrative Case Reviewer')
    clean_title.gsub!(/\bCIVIL ENGR\. I\b/i, 'Civil Engineer 1')
    clean_title.gsub!(/\bCIVIL ENGR\. IV\b/i, 'Civil Engineer 4')
    clean_title.gsub!(/\bCIVIL ENGR\. VII\b/i, 'Civil Engineer 7')
    clean_title.gsub!(/\bCLERK OF APPELLATE CT\b/i, 'Clerk of Appellate Court')
    clean_title.gsub!(/\bCOLLECTIVE BARGAING ADMIN SECTRY\b/i, 'Collective Bargaining Administrative Secretary')
    clean_title.gsub!(/\bCOMMERCE COMM POLICE OFFICER I\b/i, 'Commerce Commission Police Officer 1')
    clean_title.gsub!(/\bCOMMERCE COMM POLICE OFFICER II\b/i, 'Commerce Commission Police Officer 2')
    clean_title.gsub!(/\bCOMMUNICATIONS SYSTEMS SPECIAL\b/i, 'Communications Systems Specialist')
    clean_title.gsub!(/\bCOMP EVIDENCE RECOVRY TECH\b/i, 'Computer Evidence Recovery Technician')
    clean_title.gsub!(/\bCOMP EVIDENCE RECOVRY TECH SUPR\b/i, 'Computer Evidence Recovery Technician Supervisor')
    clean_title.gsub!(/\bCOMP OUTPUT MICROFILM OPER-LEAD\b/i, 'Computer Output Microfilm Operator-Lead')
    clean_title.gsub!(/\bCOORD OF ATHLTCS & CO-CURR ACTIV\b/i, 'Coordinator of Athletics & Co-Curricular Activities')
    clean_title.gsub!(/\bCORR IDENTIFICATION SUPERVISOR\b/i, 'Corrections Identification Supervisor')
    clean_title.gsub!(/\bCORR IDENTIFICATION TECHNICIAN\b/i, 'Corrections Identification Technician')
    clean_title.gsub!(/\bCOUNSEL TO AG SOC JUSTICE\/EQUITY\b/i, 'Counsel to Attorney General Social Justice/Equity')
    clean_title.gsub!(/\bCOURT REPORTING SERV ASST SUPERV\b/i, 'Court Reporting Services Assistant Supervisor')
    clean_title.gsub!(/\bCOURT REPORTING SERV SUPERV II\b/i, 'Court Reporting Services Supervisor 2')
    clean_title.gsub!(/\bCOURT STAT AND RESEARCH ANALY I\b/i, 'Court Statistical Research Analyst')
    clean_title.gsub!(/\bCRIMINAL INTELL ANALYST I\b/i, 'Criminal Intelligence Analyst 1')
    clean_title.gsub!(/\bCRIMINAL INTELL ANALYST II\b/i, 'Criminal Intelligence Analyst 2')
    clean_title.gsub!(/\bCRIMINAL INTELLIGENCE ANALT SPEC\b/i, 'Criminal Intelligence Analyst Specialist')
    clean_title.gsub!(/\bCUSTOMER SERV INVENT SPECIALIST\b/i, 'Customer Services Inventory Specialist')
    clean_title.gsub!(/\bDATA PROCESSING ADMIN SPEC\b/i, 'Data Processing Administrative Specialist')
    clean_title.gsub!(/\bDAY CARE LIC REP II\b/i, 'Day Care Licensing Representative 2')
    clean_title.gsub!(/\bDEP CHF OF ADMIN SERV & COMPL\b/i, 'Deputy Chief of Administrative Services & Compliance')
    clean_title.gsub!(/\bDEPUTY AG, POLICY\b/i, 'Deputy Attorney General, Policy')
    clean_title.gsub!(/\bDEPUTY CHIEF OF STAFF ADMIN\b/i, 'Deputy Chief of Staff Administration')
    clean_title.gsub!(/\bDEPUTY DIRECTOR OF ADVANCE\b/i, 'Deputy Director of Advance')
    clean_title.gsub!(/\bDIR ETHICS TRAIN & COMPLIANCE\b/i, 'Director Ethics Training & Compliance')
    clean_title.gsub!(/\bDIR FINANCIAL & PROFESSIONAL REG\b/i, 'Director Financial & Professional Regulation')
    clean_title.gsub!(/\bDIR OF AR,PRO\.DEV & DIVERSITY\b/i, 'Director of Attorney Recruiting, Professional Development & Diversity')
    clean_title.gsub!(/\bDIR OF COMMERCE & ECONOMIC OPP\b/i, 'Director of Commerce & Economic Opportunity')
    clean_title.gsub!(/\bDIR OF TECH SUPP&INFRASTRUCTURE\b/i, 'Director of Technology Support & Infrastructure')
    clean_title.gsub!(/\bDIR, ACS TO JUSTICE & STRAT PLNG\b/i, 'Director, Access to Justice & Strategic Planning')
    clean_title.gsub!(/\bDIRECTOR OF ADVANCE\b/i, 'Director of Advance')
    clean_title.gsub!(/\bDIRECTOR OF DEPT OF CORRECTION\b/i, 'Director of Department of Correction')
    clean_title.gsub!(/\bDIRECTOR OF MULTILINGUAL\b/i, 'Director of Multilingual Services')
    clean_title.gsub!(/\bDIRECTOR OF MULTILINGUAL SERVICE\b/i, 'Director of Multilingual Services')
    clean_title.gsub!(/\bDIRECTOR OF ROE ISC\b/i, 'Director of Regional Offices of Education and Intermediate Service Centers')
    clean_title.gsub!(/\bDIRECTOR OF TITLE GRANT ADMIN\b/i, 'Director of Title Grant Administration')
    clean_title.gsub!(/\bDIVISION CHIEF & ADMIN COUNSEL\b/i, 'Division Chief & Administrative Counsel')
    clean_title.gsub!(/\bDRUG COMPLIANCE INVEST\b/i, 'Drug Compliance Investigator')
    clean_title.gsub!(/\bDUNN FELLOW\b/i, 'Dunn Fellowship')
    clean_title.gsub!(/\bE S TAX AUDITOR I\b/i, 'Employment Security Tax Auditor 1')
    clean_title.gsub!(/\bE S TAX AUDITOR II\b/i, 'Employment Security Tax Auditor 2')
    clean_title.gsub!(/\bEL EQUIP INSTALL\/REPAIR LEAD WKR\b/i, 'Electronic Equipment Installer/Repairer Lead Worker ')
    clean_title.gsub!(/\bELECT EQUIP INSTALLER\/REPAIRER\b/i, 'Electronic Equipment Installer/Repairer')
    clean_title.gsub!(/\bELECTRIC FIELD INSPECTOR I\b/i, 'Electrical Field Inspector 1')
    clean_title.gsub!(/\bEMERGENCY RESPONSE LEAD TELECOMM\b/i, 'Emergency Response Lead Telecommunicator')
    clean_title.gsub!(/\bEMERGENCY RESPONSE TELECOMMUNTOR\b/i, 'Emergency Response Telecommunicator')
    clean_title.gsub!(/\bEND-USER COMPUTER SYSTEMS ANALYS\b/i, 'End-User Computer Systems Analyst')
    clean_title.gsub!(/\bENVIRON PROTECT LEGAL INVEST I\b/i, 'Environmental Protection Legal Investigator 1')
    clean_title.gsub!(/\bENVIRON PROTECT LEGAL INVEST II\b/i, 'Environmental Protection Legal Investigator 2')
    clean_title.gsub!(/\bENVIRON PROTECTION GEOL III\b/i, 'Environmental Protection Geologist 3')
    clean_title.gsub!(/\bEXEC DIR INNOVATN & SECDRY TRANS\b/i, 'Executive Director Innovation & Secondary Transformation')
    clean_title.gsub!(/\bEXEC INSPECT GEN=COMPTROLLER\b/i, 'Executive Inspector General Comptroller')
    clean_title.gsub!(/\bEXECUTIVE ASSISTANT TO THE AG\b/i, 'Executive Assistant to the Attorney General')
    clean_title.gsub!(/\bFINANCIAL INST EXAM TRAINEE\b/i, 'Financial Institutions Examiner Trainee')
    clean_title.gsub!(/\bFINANCIAL INSTITUTIONS EXAM I\b/i, 'Financial Institutions Examiner 1')
    clean_title.gsub!(/\bFINANCIAL INSTITUTIONS EXAM II\b/i, 'Financial Institutions Examiner 2')
    clean_title.gsub!(/\bFINANCIAL INSTITUTIONS EXAM III\b/i, 'Financial Institutions Examiner 3')
    clean_title.gsub!(/\bHIST LIBRARY CHIEF\/ACQUISITIONS\b/i, 'Historical Library Chief/Acquisitions')
    clean_title.gsub!(/\bHISTORIC DOCUMENTS CONSERVATOR I\b/i, 'Historical Documents Conservator 1')
    clean_title.gsub!(/\bIL LICENSED ADM PHYS\b/i, 'Illinois Licensed Administrative Physicist')
    clean_title.gsub!(/\bIL NAT GUARD STATE ACTIVE DUTY\b/i, 'Illinois National Guard State Active Duty')
    clean_title.gsub!(/\bIND & COM DEVLPMT REP II\b/i, 'Industrial & Community Development Representative 2')
    clean_title.gsub!(/\bINFO TECH MANAGER ADMIN COORD\b/i, 'Information Technology Manager Administrative Coordinator')
    clean_title.gsub!(/\bINFORMATION SERVICES TECH II\b/i, 'Information Services Technician 2')
    clean_title.gsub!(/\bINSTITUTIONAL COMPLY EXAMNIER IV\b/i, 'Institutional Compliance Examiner 4')
    clean_title.gsub!(/\bINSURANCE CO FIELD STAFF EXAMNR\b/i, 'Insurance Company Field Staff Examiner')
    clean_title.gsub!(/\bINSURANCE CO FIN EXAMINER TRN\b/i, 'Insurance Company Financial Examiner Trainee')
    clean_title.gsub!(/\bINTELL TRANS SYS FIELD TECH\b/i, 'Intelligent Transportation Systems Field Technician')
    clean_title.gsub!(/\bINTERMIT MOTOR VEHICLE CASHIER\b/i, 'Intermittent Motor Vehicle Cashier')
    clean_title.gsub!(/\bINTERMITTENT MTR VEH REG TECH I\b/i, 'Intermittent Motor Vehicle Regulations Technician 1')
    clean_title.gsub!(/\bJUVENILE JUST INDEPEND OMBUDSMAN\b/i, 'Juvenile Justice Independent Ombudsman')
    clean_title.gsub!(/\bLEAD ADVANCE REP\b/i, 'Lead Advance Representative')
    clean_title.gsub!(/\bLEAD INFORMATION SYS DEVELOPER\b/i, 'Lead Information Systems Developer')
    clean_title.gsub!(/\bLEARNING RENEWAL-HIGH IMP TUT LE\b/i, 'Learning Renewal - High Impact Tutoring Lead')
    clean_title.gsub!(/\bLEARNING RENEWAL-SOC EMO LEARN L\b/i, 'Learning Renewal-Social Emotional Learning Lead')
    clean_title.gsub!(/\bMAINT WORKER POWER PLANT\b/i, 'Maintenance Worker (Power Plant)')
    clean_title.gsub!(/\bMEDICAL ADM I OPT D\b/i, 'Medical Administrator I, Opt D')
    clean_title.gsub!(/\bMEDICAL ADM II OPT D\b/i, 'Medical Administrator II, Opt D')
    clean_title.gsub!(/\bMEDICAL ADMIN I OPT Z\b/i, 'Medical Administrator I, Opt Z')
    clean_title.gsub!(/\bMEMBER LIQ CONT COMM I\b/i, 'Member Liquor Contract Communications 1')
    clean_title.gsub!(/\bMEMEBER MERIT EMPLOY COMM\b/i, 'Member Merit Employee Communications')
    clean_title.gsub!(/\bMETHODS & PROCED ADVISOR II\b/i, 'Methods and Procedures Advisor 2')
    clean_title.gsub!(/\bMILITARY ELECTRONIC SEC TECH I\b/i, 'Military Electronic Security Technician 1')
    clean_title.gsub!(/\bMILITARY ELECTRONIC SEC TECH II\b/i, 'Military Electronic Security Technician 2')
    clean_title.gsub!(/\bMILITARY ELECTRONIC SEC TECH III\b/i, 'Military Electronic Security Technician 3')
    clean_title.gsub!(/\bMILITARY REAL PROPERY CLERK\b/i, 'Military Real Property Clerk')
    clean_title.gsub!(/\bMOTOR CARRIER RECIP\/PRORATE AUD\b/i, 'Motor Carrier Reciprocity/Prorate Auditor')
    clean_title.gsub!(/\bNUCLEAR SAF EM RESPONSE TECH II\b/i, 'Nuclear Safety Emergency Response Technician 2')
    clean_title.gsub!(/\bNUCLEAR SAFETY ADMIN OFFICER I\b/i, 'Nuclear Safety Administration Officer 1')
    clean_title.gsub!(/\bNUCLEAR SAFETY HEAL PHYS TECH I\b/i, 'Nuclear Safety Health Physics Technician 1')
    clean_title.gsub!(/\bNUCLEAR SAFETY HEAL PHYS TECH II\b/i, 'Nuclear Safety Health Physics Technician 2')
    clean_title.gsub!(/\bOFFICE COORDINATOR\/APO\b/i, 'Office Coordinator/Apo')
    clean_title.gsub!(/\bPARKING COORDINATOR\/ADMIN ASST\b/i, 'Parking Coordinator/Administrative Assistant')
    clean_title.gsub!(/\bPROCUREMENT COMPLIANCE MONITOR\b/i, 'Procurement Compliance Monitor')
    clean_title.gsub!(/\bPUBLIC ADM INTERN\b/i, 'Public Administration Intern')
    clean_title.gsub!(/\bRECORDKEEPING & TECH UNIT CSM\b/i, 'Recordkeeping & Technology Unit Court Services Manager')
    clean_title.gsub!(/\bREHAB WORKSHOP INSTRUC I\b/i, 'Rehabilitation Workshop Instructor 1')
    clean_title.gsub!(/\bREHAB WORKSHOP INSTRUC II\b/i, 'Rehabilitation Workshop Instructor 2')
    clean_title.gsub!(/\bREHABILITATION WORKSHOP SUP III\b/i, 'Rehabilitation Workshop Supervisor 3')
    clean_title.gsub!(/\bSAFETY ENVIRONMENTAL COMP ASSOC\b/i, 'Safety Environmental Compliance Associates')
    clean_title.gsub!(/\bSAFETY RESPON ANALYST SUPV\b/i, 'Safety Responsibility Analyst Supervisor')
    clean_title.gsub!(/\bSCHEDULER FOR LT GOV\b/i, 'Scheduler for Lieutenant Governor')
    clean_title.gsub!(/\bSEC THERAPY AIDE TR\b/i, 'Security Therapy Aide Trainee')
    clean_title.gsub!(/\bSECRETARY ADM TRANSCRIBING\b/i, 'Secretary Administrative Transcribing')
    clean_title.gsub!(/\bSITE ASSISTANT SUPT I\b/i, 'Site Assistant Superintendent 1')
    clean_title.gsub!(/\bSITE ASSISTANT SUPT II\b/i, 'Site Assistant Superintendent 2')
    clean_title.gsub!(/\bSMALL ENGINE MECHANIC\b/i, 'Small Engine Mechanic')
    clean_title.gsub!(/\bSPEC EVENTS & PRGRM COORDINATOR\b/i, 'Special Events & Program Coordinator')
    clean_title.gsub!(/\bSPEC'L EDUCATION RESOURCES COORD\b/i, 'Special Education Resources Coordinator')
    clean_title.gsub!(/\bSPO I\/APO\b/i, 'State Purchasing Officer 1/Agency Purchasing Officer')
    clean_title.gsub!(/\bSR PRGM MGR,ACCESS & CMNTY TRUST\b/i, 'Senior Program Manager, Access & Community Trust')
    clean_title.gsub!(/\bSR PROG MAN\/LGL TECH INITIATIVES\b/i, 'Senior Program Manager/Legal Technology Initiatives')
    clean_title.gsub!(/\bSR PROGRAM MGR, TECH & SUPPORT\b/i, 'Senior Program Manager, Technology & Support')
    clean_title.gsub!(/\bST PURCH OFF\/SPEC AD CHF PRO OFF\b/i, 'State Purchasing Officer/Specialist Advisor Chief Procurement Officer')
    clean_title.gsub!(/\bSTAT RESEARCH SPECIALIST I\b/i, 'Statistical Research Specialist 1')
    clean_title.gsub!(/\bSTAT RESEARCH SPECIALIST II\b/i, 'Statistical Research Specialist 2')
    clean_title.gsub!(/\bSTAT RESEARCH SPECIALIST III\b/i, 'Statistical Research Specialist 3')
    clean_title.gsub!(/\bSUPR SR PRGRM MGR,IL COURT HELP\b/i, 'Supervising Senior Program Manager, Illinois Court Help')
    clean_title.gsub!(/\bSUPT SAFETY INSP & ED DEPT LABO\b/i, 'Support Safety Inspector & Education Deputy Laboratory')
    clean_title.gsub!(/\bTERROR RESEARCH SPECIALIST TRAIN\b/i, 'Terrorism Research Specialist Trainee')
    clean_title.gsub!(/\bVET EMPMT REP II\b/i, "Veteran's Employment Representative 2")
    clean_title.gsub!(/\bWAREHOSE & MATRLS DISTRIBUTN SUP\b/i, 'Warehouse & Materials Distribution Supervisor')
    clean_title.gsub!(/\bWORKER COMP INS COMP INVESTIGATO\b/i, 'Worker Compensation Insurance Compliance Investigator')

    clean_title.gsub!(/\bNUCLEAR SAFETY HEAL PHYS TECH\b/i, 'Nuclear Safety Health Physics Technician')
    clean_title.gsub!(/\bIL LICENSED ADM PHYS\b/i, 'Illinois Licensed Administrative Physicist')

    clean_title.gsub!(/\bAG\b(?=\sland)/i, 'Agricultural')
    clean_title.gsub!(/\bA\s?G\b/i, 'Attorney General')
    clean_title.gsub!(/\bAcct\sFiscal\sAdmin\b/i, 'Accounting and Fiscal Administration')
    clean_title.gsub!(/\bAcc?s\b/i, 'Access')
    clean_title.gsub!(/\bOVER DEM\b/i, 'Over Dimension')

    clean_title.gsub!(/\bActiv\b/i, 'Activities')
    clean_title.gsub!(/\bAdj\b/i, 'Adjutant')
    clean_title.gsub!(/\bAdv\b/i, 'Advisor')
    clean_title.gsub!(/\b(Aff|Afrs)\b/i, 'Affairs')
    clean_title.gsub!(/\bAg(ricultural)?\sLand[&\/]Water\b/i, 'Agricultural Land & Water')
    clean_title.gsub!(/\bANIMAL&ANIMAL\b/i, 'Animal and Animal')
    clean_title.gsub!(/\bAR\b/i, 'Attorney Recruiting')
    clean_title.gsub!(/\bAsst Adj(utant?) Gen(eral)?\b/i, 'Assistant Adjutant General')
    clean_title.gsub!(/\bATT REC & LAW\b/i, 'Attorney Recruitment & Law')
    clean_title.gsub!(/\bBEAUTY CULT\b/i, 'Beauty Culture')
    clean_title.gsub!(/\bCAP DEV\b/i, 'Capital Development')
    clean_title.gsub!(/\bCAP POLICE INVEST\b/i, 'Capitol Police Investigator')

    clean_title.gsub!(/\bCOMP EVIDENCE\b/i, 'Computer Evidence')

    clean_title.gsub!(/\bCOMM\b/i, 'Commission')
    clean_title.gsub!(/\bCOMP\b/i, 'Compliance')
    clean_title.gsub!(/\bCONCEAL\b/i, 'Concealed')
    clean_title.gsub!(/\b(Concerv|Cons)\b/i, 'Conservation')
    clean_title.gsub!(/\bCondit(ion)?\b/i, 'Conditioning')
    clean_title.gsub!(/\bCORR\b/i, 'Corrections')
    clean_title.gsub!(/\bCr?t\b/i, 'Court')

    clean_title.gsub!(/\bDEM\b/i, 'Dimension')

    clean_title.gsub!(/\bDR COURTS\b/i, 'Director of Courts')
    clean_title.gsub!(/\bDst Pln & Mb\b/i, 'District Planning & Member')
    clean_title.gsub!(/\bEl(ect)? Equip\b/i, 'Electronic Equipment')
    clean_title.gsub!(/\bEM\b/i, 'Emergency')
    clean_title.gsub!(/\bEmiss\b/i, 'Emission')
    clean_title.gsub!(/\bEmploye,\b/i, 'Employee')
    clean_title.gsub!(/\bEngage\b/i, 'Engagement')
    clean_title.gsub!(/\bEQ REPAIR\b/i, 'Equipment Repair')
    clean_title.gsub!(/\bEq\b/i, 'Equipment')
    clean_title.gsub!(/\bFAM\b/i, 'Families')
    clean_title.gsub!(/\bFAMILYDIV\b/i, 'Family Division')
    clean_title.gsub!(/\bGOV\b/i, 'Governor')
    clean_title.gsub!(/\bHeal\b/i, 'Health')
    clean_title.gsub!(/\bIL\b/i, 'Illinois')
    clean_title.gsub!(/\bIND\b/i, 'Industrial')
    clean_title.gsub!(/\bINS COMP\b/i, 'Insurance Compliance')
    clean_title.gsub!(/\bINST\b/i, 'Institutions')
    clean_title.gsub!(/\bINSTRUC\b/i, 'Instructor')
    clean_title.gsub!(/\bINVT\b/i, 'Investment')
    clean_title.gsub!(/\bJC\b/i, 'Judicial Conference')
    clean_title.gsub!(/\bLE\b/i, 'Lead')
    clean_title.gsub!(/\bLearn L\b/i, 'Learning Lead')
    clean_title.gsub!(/\bLIC\b/i, 'Licensing')
    clean_title.gsub!(/\bLIQ\b/i, 'Liquor')
    clean_title.gsub!(/\bMech\b/i, 'Mechanical')
    clean_title.gsub!(/\bMsgng\b/i, 'Messaging')
    clean_title.gsub!(/\bOPER\b/i, 'Operation')
    clean_title.gsub!(/\bOPP\b/i, 'Opportunity')
    clean_title.gsub!(/\bOPT OUT\b/i, 'Opt-out')
    clean_title.gsub!(/\bOpts Dr\b/i, 'Operations Director')
    clean_title.gsub!(/\bP A\b/i, 'Public Aid')
    clean_title.gsub!(/\bPET SER\b/i, 'Pretrial Services')
    clean_title.gsub!(/\bPLANT & PESTICIDE\b/i, 'Plant and Pesticide')
    clean_title.gsub!(/\b(Pprd|Prepard)\b/i, 'Preparedness')
    clean_title.gsub!(/\bPRO\.DEV\b/i, 'Professional Development')
    clean_title.gsub!(/\bPrtrl\b/i, 'Pretrial')
    clean_title.gsub!(/\bRE\b/i, 'Recruitment')
    clean_title.gsub!(/\bRecip\b/i, 'Reciprocity')
    clean_title.gsub!(/\bREG\b/i, 'Regulation')
    clean_title.gsub!(/\bRev\b/i, 'Revenue')
    clean_title.gsub!(/\bSaf\b/i, 'Safety')
    clean_title.gsub!(/\bSaf Em\b/i, 'Safety Emergency')
    clean_title.gsub!(/\bSafety Heal\b/i, 'Safety Health')
    clean_title.gsub!(/\bSEC THERAPY AIDE\b/i, 'Security Therapy Aide')
    clean_title.gsub!(/\bSec(ur)?\b/i, 'Security')
    clean_title.gsub!(/\bSECDRY TRANS\b/i, 'Secondary Transformation')
    clean_title.gsub!(/\bSPEC EVENTS\b/i, 'Special Events')
    clean_title.gsub!(/\bSPRT\b/i, 'Support')
    clean_title.gsub!(/\b(Spr(vr?)?|Spr?vsr)\b/i, 'Supervisor')
    clean_title.gsub!(/\bSTAT\b/i, 'Statistical')
    clean_title.gsub!(/\bSTRAT PLNG\b/i, 'Strategic Planning')
    clean_title.gsub!(/\b(Strat|Strt?gc)\b/i, 'Strategic')
    clean_title.gsub!(/\bTUT\b/i, 'Tutoring')
    clean_title.gsub!(/\bTut Le\b/i, 'Tutoring Lead')

    clean_title.gsub!(/\bAPO\b/i, 'Agency Procurement Officer')
    clean_title.gsub!(/\bACP\b/i, 'Address Confidentiality Program')
    clean_title.gsub!(/\bSPO\b/i, 'State Purchasing Officer ')

    clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title)

    clean_title.gsub!(/\bALPLM\b/i, 'Abraham Lincoln Presidential Library and Museum (ALPLM)')
    clean_title.gsub!(/\bBAIID\b/i, 'Breath Alcohol Ignition Device (BAIID)')
    clean_title.gsub!(/\bCDL\b/i, 'Commercial Drivers License (CDL)')
    clean_title.gsub!(/\bCII\b/i, 'CII')
    clean_title.gsub!(/\bCLE\b/i, 'Continuing Legal Education (CLE)')
    clean_title.gsub!(/\bCRS\b/i, 'Court Reporters')
    clean_title.gsub!(/\bCRSA\b/i, 'Community & Residential Services Authority (CRSA)')
    clean_title.gsub!(/\bDBE\b/i, 'Disadvantaged Business Enterprise (DBE)')
    clean_title.gsub!(/\bDEI\b/i, 'Diversity, Equity, and Inclusion (DEI)')
    clean_title.gsub!(/\bDHS\b/i, 'Department of Human Services (DHS)')
    clean_title.gsub!(/\bDRC\b/i, 'Defender Resource Center (DRC)')
    clean_title.gsub!(/\bE\s?P\s?A\b/i, 'Environmental Protection Agency')
    clean_title.gsub!(/\bE S\b/i, 'Employment Security')
    clean_title.gsub!(/\bESSA\b/i, 'Every Student Succeeds Act (ESSA)')
    clean_title.gsub!(/\bETC\b/i, 'General Manager Electronic Toll Collection (ETC)')
    clean_title.gsub!(/\bGAAP\b/i, 'Generally Accepted Accounting Principles (GAAP)')
    clean_title.gsub!(/\bGOCA\b/i, "Governor's Office of Constituent Affairs")
    clean_title.gsub!(/\bICAC\b/i, 'Internet Crimes Against Children (ICAC)')
    clean_title.gsub!(/\bIDES\b/i, 'International Disaster Emergency Service (IDES)')
    clean_title.gsub!(/\bIFVCC\b/i, 'Illinois Family Violence Coordinating Councils (IFVCC)')
    clean_title.gsub!(/\bISC\b/i, 'Intermediate Service Centers')
    clean_title.gsub!(/\bIVPA\b/i, 'International Volunteer Programs Association (IVPA)')
    clean_title.gsub!(/\bIVRS\b/i, 'Interactive Voice Response (IVRS)')
    clean_title.gsub!(/\bOSPS\b/i, 'Office of Statewide Pretrial Services (OSPS)')

    clean_title.gsub!(/\bAAG\b/i, 'AAG')
    clean_title.gsub!(/\bAVN\b/i, 'AVN')
    clean_title.gsub!(/\bCIS\b/i, 'CIS')
    clean_title.gsub!(/\bCSM\b/i, 'Court Services Manager (CSM)')
    clean_title.gsub!(/\bCTE\b/i, 'CTE')
    clean_title.gsub!(/\bDAB\b/i, 'DAB')
    clean_title.gsub!(/\bEEO\b/i, 'EEO')
    clean_title.gsub!(/\bEEO\/AA\/ADA\b/i, 'EEO/AA/ADA')
    clean_title.gsub!(/\bFoia\b/i, 'FOIA')
    clean_title.gsub!(/\bGata\b/i, 'GATA')
    clean_title.gsub!(/\bGIS\b/i, 'GIS')
    clean_title.gsub!(/\bIPLEM\b/i, 'IPLEM')
    clean_title.gsub!(/\bJULIE\b/i, 'JULIE')
    clean_title.gsub!(/\bMNO\b/i, 'MNO')
    clean_title.gsub!(/\bOES\b/i, 'OES')
    clean_title.gsub!(/\bORT\b/i, 'ORT')
    clean_title.gsub!(/\bP S A\b/i, 'PSA')
    clean_title.gsub!(/\bPCM\b/i, 'PCM')
    clean_title.gsub!(/\bPIC\b/i, 'PIC')
    clean_title.gsub!(/\bSPS\b/i, 'SPS')
    clean_title.gsub!(/\bVAWA\b/i, 'VAWA')
    clean_title.gsub!(/\bVOCA\b/i, 'VOCA')

    puts clean_title
    puts "#{item[:raw_column]} >>> #{clean_title}".cyan if item[:raw_column] != clean_title
    insert_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_title)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    # puts insert_query
    route.query(insert_query)
  end
end

def clean_cities_and_zips(route)
  # move_zips(route)
  # move_cities(route)
  # split_cities_from_location(route)
  match_cities(route)
end

def move_zips(route)
  up_query = <<~MySQL
    UPDATE il_raw.il_gov_employee_salaries__locations_matched
    SET zip5=LEFT(zip, 5)
    WHERE zip IS NOT NULL
    AND zip5 IS NULL;
  MySQL
  route.query(up_query)
end

def move_cities(route)
  up_query = <<~MySQL
    UPDATE il_raw.il_gov_employee_salaries__locations_matched
    SET city_for_matching=TRIM(city)
    WHERE city IS NOT NULL
    AND city_for_matching IS NULL;
  MySQL
  route.query(up_query)
end

def split_cities_from_location(route)
  query = <<~MySQL
    SELECT DISTINCT location
    FROM il_raw.il_gov_employee_salaries__locations_matched
    WHERE city_for_matching IS NULL
      AND location IS NOT NULL;
  MySQL
  city_list = route.query(query, symbolize_keys: true).to_a
  city_list.each do |item|
    m = item[:location].match(/((?<=^)|(?<=\s))(?<city>[a-z ]*+)(,?\s*)(?<=\s)?(?<state>\b\w{2}\b)?(,?\s*)?(?<zip>\d{5}(-\d{4})?)?$/i)
    next if m.nil?

    city = m[:city]
    state = m[:state]
    zip = m[:zip].nil? ? nil : m[:zip][0..4]
    up_query = <<~MySQL
      UPDATE il_raw.il_gov_employee_salaries__locations_matched
      SET city_for_matching = #{escape_or_null(city)},
          state_for_matching = #{escape_or_null(state.upcase)},
          zip5 = #{escape_or_null(zip)}
      WHERE location=#{escape_or_null(item[:location])}
        AND city_for_matching IS NULL;
    MySQL
    # puts up_query
    route.query(up_query)
  end
end

def match_cities(route)
  query = <<~MySQL
    SELECT DISTINCT city_for_matching, state_for_matching
    FROM il_raw.il_gov_employee_salaries__locations_matched
    WHERE city_for_matching IS NOT NULL
      AND city_org_id IS NULL
      AND (state_for_matching='IL' OR state_for_matching IS NULL);
  MySQL
  city_list = route.query(query, symbolize_keys: true).to_a
  return if city_list.empty?

  city_list.each do |item|
    city_name = item[:city_for_matching].dup.squeeze(' ')
                  .split(/\b/).map(&:capitalize).join
                  .sub(/\bhts\.?\b/i, 'Heights')
                  .sub(/\btwn?s?p\.?\b/i, 'Township')
                  .sub(/\bjct\.?\b/i, 'Junction')
                  .sub(/\bSPG\b/i, 'Springs')
    cn =
      if city_name.length > 5
        MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name.dup, 'IL', 1)
      end
    city_name = cn if cn

    city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name))
    city_org_id = get_city_org_id(city_name, route)

    and_state_for_matching = item[:state_for_matching].nil? ? ' IS NULL' : "='IL'"

    update_query = <<~SQL
      UPDATE il_gov_employee_salaries__locations_matched
      SET city_for_matching='#{escape(city_name)}',
          state_for_matching = '#{city_org_id.nil? ? item[:state_for_matching] : "IL"}',
          city_org_id = #{city_org_id.nil? ? "NULL" : "#{city_org_id}"}
      WHERE city_org_id IS NULL
        AND city_for_matching='#{escape(item[:city_for_matching])}'
        AND state_for_matching#{and_state_for_matching};
    SQL
    puts update_query.red
    route.query(update_query)
  end
end

def get_city_org_id(city, route)
  query = <<~SQL
    SELECT pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name='Illinois'
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND not_distinguishable_by_name=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    nil
  else
    res.first['pl_production_org_id']
  end
end
