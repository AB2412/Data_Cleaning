# Creator:      Sergii Butrymenko
# Dataset Name: IL Teachers' Retirement System
# Task #:       21
# Created:      April 2021

# ruby mlc.rb --tool="clean::il::il_teachers_retirement"

IL_FULL = {
    'Alex-Pulaski Car G Tr A' =>	'Mount Pulaski Community School District 23',
    'Asst Co Supt' => 'Illinois Career Education Area Service Center',
    'Drug Free School & Trcy Prog' => 'Illinois Board of Education',
    'IL Insti Dev Disabilities' => 'Illinois Institute on Disability and Human Development ',
}

IL_COUNTIES = {
    'Alxndr' =>	'Alexander',
    'Clk' => 'Clark',
    'Cly' => 'Clay',
    'Clin' => 'Clinton',
    'Cls' => 'Coles',
    'Cfrd' => 'Crawford',
    'Cmbn' => 'Cumberland',
    'Dewitt' => 'De Witt',
    'Dg' => 'Douglas',
    'Dupage' => 'DuPage',
    '-Ed-' => '-Edgar-',
    'Edwd' => 'Edwards',
    'Gltn' => 'Gallatin',
    'Grne' => 'Greene',
    'Hdin' => 'Hardin',
    'Jspr' => 'Jasper',
    'Jrsy' => 'Jersey',
    'JoDav' => 'Jo Daviess',
    'John' => 'Johnson',
    'La Salle' => 'LaSalle',
    'Lwrn' => 'Lawrence',
    'Mar' => 'Marion',
    'Masc' => 'Massac',
    'Mltr' => 'Moultrie',
    'Po' => 'Pope',
    'Plski' => 'Pulaski',
    'Rlnd' => 'Richland',
    'Sln' => 'Saline',
    'Sh' => 'Shelby',
    'St Clair' => 'St. Clair',
    'Stph' => 'Stephenson',
    'Un' => 'Union',
    'Wbh' => 'Wabash',
    'Was' => 'Washington',
}

IL_SHORTS =
    {'CUSD' => 'Community Unit School District',
    'CUD' => 'Community Unit District',
    'C U' => 'Community Unit',
    'UD' => 'Unit District',
    'CHSD' => 'Community High School District',
    'H S' => 'High School',
    'CESD' => 'Community Elementary School District',
    'CHS' => 'Community High School',
    'CSD' => 'Consolidated School District',
    'CCD' => 'Community Consolidated District',
    'CCSD' => 'Community Consolidated School District',
    'C C S D' => 'Community Consolidated School District',
    'THSD' => 'Township High School District',
    'PAEC' => 'Proviso Area for Exceptional Children',
    'SEJA' => 'Special Education Cooperative Joint Agreement',
    'Sma' => 'SMA',
    'ESD' => 'Elementary School District',
    'USD' => 'Unit School District',
    'ROE' => 'Regional Office of Education',
    'SD' => 'School District',
    'GSD' => 'Grade School District',
    'CTE' => 'Career and Technical Education',
    'SD146' => 'School District 146',
    'S D' => 'School District',
    'HSD' => 'High School District',
    'TWP' => 'Township',
    'TRS' => 'Teachers\' Retirement System',
    'ECCD' => 'Early Childhood Care and Development',
    'ICRE' => 'Illinois Center for Rehabilitation and Education',
    'DCFS' => 'Department of Children and Family Services',
    'ISBE' => 'Illinois State Board of Education',
    'Diec' => 'DIEC',
    'Cna' => 'CNA',
    'Ctr' => 'Center',
    'Interm' => 'Intermediate',
    'Non-Occup' => 'Nonoccupational',
    'Cons' => 'Consolidated',
    'Cnty' => 'County',
    'Co' => 'County',
    'Pa' => 'Park',
    'Cntys' => 'Counties',
    }

IL_OTHERS =
    {'Hazlgrn'  => 'Hazelgreen',
    'Oaklwn' => 'Oak Lawn',
    'N Pekin' => 'North Pekin',
    'A C Central' => 'A-C Central',
    'A L Bowen' => 'A.L. Bowen',
    'Wm Holliday' => 'William Holliday',
    'Elem' => 'Elementary',
    'Spec' => 'Special',
    'Reg' => 'Regional',
    'Hs' => 'High School',
    'Ed' => 'Education',
    'Educ' => 'Education',
    'Serv' => 'Service',
    'Coop' => 'Cooperative',
    'Voc' => 'Vocational',
    'IL' => 'Illinois',
    'Bsmck' => 'Bismarck',
    'Henng' => 'Henning',
    'Rossvl' => 'Rossville',
    'Alv' => 'Alvin',
    'Collab' => 'Collaborative',
    'Mt' => 'Mt.',
    'At' => 'at',
    }

def execute(options = {})
  where_part = options['where']
  employee_names_cleaning(where_part)
end

def escape(str)
  return nil if str.nil?
  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #21] IL Teachers' Retirement System* \n>#{message}",
      as_user: true
  )
end

def insert(db, tab, h, ignore = false, log=false)
  query = <<~SQL
    INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')})
    VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});
  SQL
  puts query.red if log
  # db.query(query)
end

def get_recent_date(table, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table doesn't exist. Creating *#{table}* now...")
    create_table = <<~SQL
      CREATE TABLE #{table}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         name VARCHAR(255) NOT NULL,
         clean_name VARCHAR(255),
         scrape_date DATE,
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         UNIQUE (name));
    SQL
    route.query(create_table)
    puts 'Table created'
    recent_date = Date.new(2021, 3, 1)
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_names_table(table, recent_date, route)
  query = <<~SQL
    SELECT last_employer AS name, MIN(DATE(created_at)) AS scrape_date
    FROM teachers_retirement_system_retired_roster_2020 r
      LEFT JOIN #{table} cl on r.last_employer = cl.name
    WHERE cl.name IS NULL
      AND DATE(created_at) >= '#{recent_date}'
    GROUP BY r.last_employer
    UNION
    SELECT last_employer, MIN(DATE(created_at)) AS scrape_date
    FROM teachers_retirement_system_active_roster_2020 a
      LEFT JOIN #{table} cl on a.last_employer = cl.name
    WHERE cl.name IS NULL
      AND DATE(created_at) >= '#{recent_date}'
    GROUP BY a.last_employer;
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack 'No new employee names found in source tables'
  else
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table} (name, scrape_date)
      VALUES
    SQL
    names_list.each do |item|
      next if item['name'].nil?
      insert_query << "('#{escape(item['name'])}','#{item['scrape_date']}'),"
    end
    insert_query = insert_query.chop + ';'
    puts insert_query.red
    route.query(insert_query)
  end
end

def clean_names(table, route)
  query = <<~SQL
    SELECT name
    FROM #{table}
    WHERE clean_name IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table}* table."
    return
  end
  regex0 = Regexp.union(IL_FULL.keys)
  regex1 = Regexp.union(IL_COUNTIES.keys)
  regex2 = Regexp.union(IL_SHORTS.keys)
  regex3 = Regexp.union(IL_OTHERS.keys)
  names_list.each do |row|
    clean_name = row
    puts "#{clean_name['name']}".cyan
    # puts "1: #{regex3}".cyan
    result_name = clean_name['name'].gsub(/\b#{regex0}\b/, IL_FULL).gsub(/\b#{regex1}\b/, IL_COUNTIES).gsub(/\b#{regex2}\b/, IL_SHORTS).gsub(/\b#{regex3}\b/, IL_OTHERS)
    # puts "2: #{result_name}".yellow
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name)
    # puts "3: #{result_name}".green
    # result_name = row[table_info[:raw_name_column]].gsub(/(?<=[a-z])(\?|�)(?=s)/i, "'").strip
    # while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
    #   result_name = result_name[1..-1].strip
    # end
    # result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
    # case result_name.count('"')
    # when 1
    #   result_name = result_name.sub('"', '')
    # when 2
    #   result_name = result_name.sub('", "', ', ')
    # else
    #   nil
    # end
    # clean_name[table_info[:clean_name_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC')
    # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_name_column]].match?(/[a-z]/i)
    clean_name['clean_name'] = result_name
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table}
      SET clean_name='#{escape(clean_name['clean_name'])}'
      WHERE clean_name IS NULL
        AND name='#{escape(clean_name['name'])}'
    SQL
    route.query(update_query)
  end
end

def employee_names_cleaning(where_part)
  db01 = C::Mysql.on(DB01, 'foia')
  recent_date = get_recent_date('teachers_retirement_system__employer_clean', db01)
  fill_names_table('teachers_retirement_system__employer_clean', recent_date, db01)
  clean_names('teachers_retirement_system__employer_clean', db01)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end
