# Creator:      Sergii Butrymenko
# Dataset Name: GA Employee Compensation
# Task #:       61
# Created:      2020
# Migrated:     June 2021

# ruby mlc.rb --tool="clean::ga::ga_employee_compensation"
# ruby mlc.rb --tool="clean::ga::ga_employee_compensation" --mode='name'
# ruby mlc.rb --tool="clean::ga::ga_employee_compensation" --mode='organization'
# ruby mlc.rb --tool="clean::ga::ga_employee_compensation" --mode='title'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    name: {
      raw_table: 'ga_employee_compensation',
      clean_table: 'ga_employee_compensation__full_names_clean',
      raw_column: 'full_name',
      clean_column: 'full_name_clean',
    },
    organization: {
      raw_table: 'ga_employee_compensation',
      clean_table: 'ga_employee_compensation__organizations_clean',
      raw_column: 'organization',
      clean_column: 'organization_clean',
      local_connection: true
    },
    title: {
      raw_table: 'ga_employee_compensation',
      clean_table: 'ga_employee_compensation__titles_clean',
      raw_column: 'title',
      clean_column: 'title_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym

  table_info = table_description[mode]
  case mode
  when :name
    names_cleaning(table_info, where_part, route)
  when :organization
    organizations_cleaning(table_info, where_part, route)
  when :title
    job_titles_cleaning(table_info, where_part, route)
  else
    names_cleaning(table_description[:name], where_part, route)
    organizations_cleaning(table_description[:organization], where_part, route)
    job_titles_cleaning(table_description[:title], where_part, route)
  end
  route.close
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
    text: "*[CLEANING #61] GA Employee Compensation* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def insert(db, tab, h, ignore = false, log=false)
  query = "INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')}) VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});"
  p query if log
  db.client.query(query)
end

def names_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_person_names(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def organizations_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_orgs(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def job_titles_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_job_titles(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
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
    local_connection = if table_info[:local_connection]
                         <<~SQL
                          organization_category VARCHAR(255),
                          org_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
                          state VARCHAR(50) DEFAULT 'Georgia',
                         SQL
                       else
                         ''
                       end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         #{local_connection}
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE (#{table_info[:raw_column]}))
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date} 
      #{"AND #{where_part}" if where_part} 
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack("No new records for *#{table_info[:raw_column]}* column found in source tables", :info)
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?
        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_person_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  names_list.each do |row|
    clean_name = row
    result_name = row[table_info[:raw_column]].gsub('_', ', ')
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    # Special WI Cleaning
    # if result_name.match?(/\bthe\b.*\bestate\b.*\bof\b/i)
    #   estate_of = result_name.match(/\bthe\b/i)[0] + ' ' + result_name.match(/\bestate\b/i)[0] + ' ' + result_name.match(/\bof\b/i)[0]
    #   result_name = result_name.sub(/\bthe\b/i, '').sub(/\bestate\b/i, '').sub(/\bof\b/i, '')
    # elsif result_name.match?(/\bestate\b.*\bof\b/i)
    #   estate_of = result_name.match(/\bestate\b/i)[0] + ' ' + result_name.match(/\bof\b/i)[0]
    #   result_name = result_name.sub(/\bestate\b/i, '').sub(/\bof\b/i, '')
    # else
    #   estate_of = nil
    # end
    # Special WI Cleaning
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
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
end

def clean_orgs(table_info, route)
  # emp_shorts= {
  #   'Aca' => 'Academy',
  #   'Admin' => 'Administration',
  #   'Wi' => 'Wisconsin',
  #   'El' => 'Elementary',
  #   'Co' => 'County',
  #   'Com' => 'Commission',
  #   'Comm' => 'Commission',
  #   'Commish' => 'Commission',
  #   'Sch D' => 'School District',
  #   'Sch Di' => 'School District',
  #   'Sch Dis' => 'School District',
  #   'Educ Communications' => 'Educate Communications',
  #   'W/ Devlp Dis' => 'with Developmental Disabilities',
  #   'Ed Serv' => 'Educational Service',
  #   'Prep Sch' => 'Preparatory School',
  #   'Agric Trade Consumer Prot' => 'Agriculture, Trade and Consumer Protection',
  #   'Dept Of Ag Trade Consume Pro' => 'Department of Agriculture, Trade and Consumer Protection',
  #   'Dept Of Safety And Pro Service' => 'Department of Safety and Professional Services',
  #   'Sch For Early Dvlpt & Achieve' => 'School for Early Development & Achievement',
  #
  #   'Cdeb' => 'Children with Disabilities Education Board',
  #   'Dlh' => 'Darrell Lynn Hines',
  #   'Accountabilty' => 'Accountability',
  #   'Milw' => 'Milwaukee',
  #   'Hi' => 'High School',
  #
  #   'AUTH' => 'AUTHORITY',
  #   'COM' => 'COMMISSION',
  #   'COMM' => 'COMMISSION',
  #   'ENV' => 'ENVIRONMENTAL',
  #   'GRNDS' => 'GROUNDS',
  #   'NAT. ' => 'NATURAL ',
  #   'NEED' => 'NEEDS',
  #   'OFF' => 'OFFICE',
  #   'PROB' => 'PROBATION',
  #   'REC' => 'RECRIATION',
  #   'S C' => 'SOUTH CAROLINA',
  #   'SC' => 'SOUTH CAROLINA',
  #   'SPEC' => 'SPECIAL',
  #   'SVC' => 'SERVICES',
  #   'SYS' => 'SYSTEM',
  #   'TEC' => 'TECHNICAL',
  #   'TECH' => 'TECHNICAL',
  #   'TRNING' => 'TRAINING',
  #
  #   'CONFED' => 'CONFEDERATE',
  #   'RM' => 'ROOM',
  #   'MIL' => 'MILITARY',
  # }
  # emp_final= {
  #   'Of' => 'of',
  #   'And' => 'and',
  #   'For' => 'for',
  #   'Uhs' => 'UHS',
  #   'Syst' => 'System',
  #   'Wis' => 'Wisconsin',
  #   'Wisc' => 'Wisconsin',
  #   'Board On Aging Long Term Care' => 'Board on Aging and Long Term Care',
  #   'Board-aging-long Term Care' => 'Board on Aging and Long Term Care',
  #   'Safety And Professional Serv' => 'Safety and Professional Services',
  #   'Sch For Early Dvlpt & Achieve' => 'School for Early Development and Achievement',
  #   'School For Early Development Andachievement' => 'School for Early Development and Achievement',
  #
  #
  #   'ARTS' => 'Arts',
  #   'Sled' => 'SLED',
  #   'John DE LA Howe' => 'John de la Howe',
  # }
  #
  # regex_emp_shorts = /\b#{emp_shorts.keys.join('\b|\b')}\b/
  # regex_emp_final = /\b#{emp_final.keys.join('\b|\b')}\b/

  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  orgs_list = route.query(query).to_a
  if orgs_list.empty?
    message_to_slack("There is no any new organization in *#{table_info[:clean_table]}* table.", :info)
    return
  end
  orgs_list.each do |row|
    clean_org = row
    puts "#{clean_org[table_info[:raw_column]]}".cyan
    # org = clean_org[table_info[:raw_column]].gsub(regex_emp_shorts, emp_shorts)
    org = clean_org[table_info[:raw_column]]
    if org.include?(',') && org.end_with?('OF', 'GA', 'GEORGIA', 'STATE')
      parts = org.rpartition(',')
      org = "#{parts.last} #{parts.first}".strip.squeeze(' ')
    end
    org = MiniLokiC::Formatize::Cleaner.org_clean(org)
    clean_org[table_info[:clean_column]] = org
    # clean_org[table_info[:clean_column]] = org.gsub(regex_emp_final, emp_final).gsub(/[-|\/][a-z]/, &:upcase).gsub(/(?<=\b|\d)1st\b/i, '1st')
    puts JSON.pretty_generate(clean_org).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_org[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_org[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
end

def clean_job_titles(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  job_titles_list = route.query(query).to_a
  if job_titles_list.empty?
    message_to_slack("There is no any new job titles in *#{table_info[:clean_table]}* table.", :info)
    return
  end
  job_titles_list.each do |row|
    clean_job_title = row
    puts "#{clean_job_title[table_info[:raw_column]]}".cyan
    puts "#{clean_job_title[table_info[:raw_column]]}".class
    clean_job_title[table_info[:clean_column]] =
      if row[table_info[:raw_column]].empty?
        ''
      else
        MiniLokiC::Formatize::Cleaner.job_titles_clean(row[table_info[:raw_column]].split('(')[0].strip)
      end
    # puts JSON.pretty_generate(clean_job_title).yellow
    puts clean_job_title[table_info[:raw_column]].yellow
    puts " " * 5 + clean_job_title[table_info[:clean_column]].green
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_job_title[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_job_title[table_info[:raw_column]])}';
    SQL
    # puts update_query.yellow
    route.query(update_query)
  end
end
