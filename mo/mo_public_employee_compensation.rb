# Creator:      Sergii Butrymenko
# Dataset Name: MO Public Employee Compensation
# Task #:       35
# Created:      2020
# Migrated:     June 2021

# ruby mlc.rb --tool="clean::mo::mo_public_employee_compensation"

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
      name: {
          raw_table: 'MO_public_employee_salary',
          clean_table: 'MO_public_employee_salary__emp_clean_names',
          raw_column: 'name',
          clean_column: 'name_cleaned',
      },
      agency: {
          raw_table: 'MO_public_employee_salary',
          clean_table: 'MO_public_employee_salary__agencies',
          raw_column: 'agency_name',
          clean_column: 'agency_name_clean',
      }
  }

  mode = options['mode']&.to_sym

  table_info = table_description[mode]
  case mode
  when :name
    names_cleaning(table_info, route)
  when :agency
    agencies_checking(table_info, route)
  else
    names_cleaning(table_description[:name], route)
    agencies_checking(table_description[:agency], route)
  end
  route.close
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #35] MO Public Employee Compensation* \n>#{message}",
      as_user: true
  )
end

def names_cleaning(table_info, route)
  names_to_clean = get_names_to_clean(table_info, route)
  if names_to_clean.empty?
    message_to_slack("There is no any new names in source table.")
    return
  end

  names_to_clean.each do |name|
    clean_name = name
    clean_name['name_cleaned'] = MiniLokiC::Formatize::Cleaner.person_clean(name['name'])
    puts JSON.pretty_generate(clean_name).yellow
    insert(route, table_info[:clean_table], clean_name, true)
  end
end

def escape(str)
  str = str.to_s.strip.squeeze(' ')
  return if str == ''
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def insert(db, tab, h, ignore = false, log=false)
  query = "INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')}) VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});"
  p query if log
  db.query(query)
end

def get_names_to_clean(table_info, route)
  query = <<~SQL
    SELECT emp_name AS name, MIN(DATE(created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} c ON r.emp_name=c.name
    WHERE DATE(created_at)>=(SELECT MAX(scrape_date) FROM #{table_info[:clean_table]})
      AND c.name IS NULL
    GROUP BY emp_name;
  SQL
  puts query.green
  route.query(query).to_a
end

def agencies_checking(table_info, route)
  query = <<~SQL
    SELECT r.agency_name
    FROM #{table_info[:raw_table]} r
         LEFT JOIN #{table_info[:clean_table]} c ON r.agency_name=c.agency_name
    WHERE c.agency_name IS NULL
    GROUP BY r.agency_name;
  SQL
  puts query.green
  agency_list = route.query(query).to_a
  if agency_list.empty?
    nil
  else
    message_to_slack "Agency(es) *#{agency_list.map{|i| i['applicant_country']}.join(', ')}* is/are absent in *db01.usa_raw.#{table_info[:clean_table]}* table. Please check and update!"
  end
end
