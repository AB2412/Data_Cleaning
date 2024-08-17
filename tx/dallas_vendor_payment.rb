# Creator:      Sergii Butrymenko
# Dataset Name: Dallas Vendor Payment
# Task #:       1
# Created:      2021
# Migrated:     May 2021

# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment"
# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment" --mode='vendor'
# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment" --mode='department'
# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment" --mode='description'
# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment" --mode='activity'
# ruby mlc.rb --tool="clean::tx::dallas_vendor_payment" --where='run_id>=1'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
      vendor: {
          raw_table: 'dallas_vendor_payment',
          clean_table: 'dallas_vendor_payment__vendors_clean',
          raw_column: 'vendor',
          clean_column: 'vendor_cleaned',
      },
      department: {
          raw_table: 'dallas_vendor_payment',
          clean_table: 'dallas_vendor_payment__departments_clean',
          raw_column: 'department',
          clean_column: 'department_cleaned',
      },
      description: {
          raw_table: 'dallas_vendor_payment',
          clean_table: 'dallas_vendor_payment__descriptions_clean',
          raw_column: 'description',
          clean_column: 'description_cleaned',
      },
      activity: {
          raw_table: 'dallas_vendor_payment',
          clean_table: 'dallas_vendor_payment__activities_clean',
          raw_column: 'activity',
          clean_column: 'activity_cleaned',
      },
      fund_type: {
          raw_table: 'dallas_vendor_payment',
          clean_table: 'dallas_vendor_payment__fund_type_clean',
          raw_column: 'fund_type',
          clean_column: 'fund_type_cleaned',
      },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :vendor
    vendors_cleaning(table_info, where_part, route)
  when :department
    departments_cleaning(table_info, where_part, route)
  when :description
    descriptions_cleaning(table_info, where_part, route)
  when :activity
    activities_cleaning(table_info, where_part, route)
  when :fund_type
    fund_types_cleaning(table_info, where_part, route)
  else
    vendors_cleaning(table_description[:vendor], where_part, route)
    departments_cleaning(table_description[:department], where_part, route)
    descriptions_cleaning(table_description[:description], where_part, route)
    activities_cleaning(table_description[:activity], where_part, route)
    fund_types_checking(table_description[:fund_type], where_part, route)
  end
  route.close
end

def vendors_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_vendors(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def departments_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_departments(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def descriptions_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_descriptions(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def activities_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_activities(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def fund_types_checking(table_info, where_part, route)
  check_fund_types(table_info, where_part, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #1] Dallas Vendor Payment* \n>#{message}",
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
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    local_connection = if table_info[:local_connection]
                         <<~SQL
                          employer_category VARCHAR(255),
                          emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
                          state VARCHAR(50) DEFAULT 'Texas',
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
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{local_connection}
         UNIQUE (#{table_info[:raw_column]}));
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
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && where_part.nil? == true} 
      #{"AND #{where_part}" if where_part} 
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables"
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
      insert_query = insert_query.chop + ';'
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_vendors(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new vendor name in *#{table_info[:clean_table]}* table."
    return
  end
  det = MiniLokiC::Formatize::Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    name_to_clean = clean_name['vendor']
    clean_name['name_type'] = det.determine(name_to_clean)
    if clean_name['name_type'] == 'Person'
      clean_name['vendor_cleaned'] =  MiniLokiC::Formatize::Cleaner.person_clean(name_to_clean, false)
    else
      clean_name['vendor_cleaned'] = MiniLokiC::Formatize::Cleaner.org_clean(name_to_clean)
    end

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

def clean_departments(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  dpts_to_clean = route.query(query).to_a
  if dpts_to_clean.empty?
    message_to_slack "There is no any new department name in *#{table_info[:clean_table]}* table."
    return
  end
  dpts_to_clean.each do |row|
    clean_name = row
    # name_to_clean = clean_name['department']
    clean_name['department_cleaned'] = MiniLokiC::Formatize::Cleaner.org_clean(clean_name['department'])

    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack "#{dpts_to_clean.count} new department name(s) added to *db01.usa_raw.#{table_info[:clean_table]}* table."
end

def clean_descriptions(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  descriptions_list = route.query(query).to_a
  if descriptions_list.empty?
    message_to_slack "There is no any new description in *#{table_info[:clean_table]}* table."
    return
  end
  total_descriptions = descriptions_list.count
  descriptions_list.each do |item|
    prepared_description = item
    cleaned_description = item['description'].capitalize
    if cleaned_description.length >= 60
      cleaned_description = cleaned_description.split(':')[0].split(' (incl')[0].split(/\s*\((?!.*\))/)[0]
    end
    prepared_description['description_cleaned'] = cleaned_description.split(' (not otherwi')[0].split(' (see')[0].split(', all types')[0].split(', all kinds')[0].chomp(',').split(/\s*\((?!.*\))/)[0]

    puts JSON.pretty_generate(prepared_description).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(prepared_description[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(prepared_description[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack "#{total_descriptions} new description(s) added to *db01.usa_raw.#{table_info[:clean_table]}* table."
end

def clean_activities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  activities_list = route.query(query).to_a
  if activities_list.empty?
    message_to_slack "There is no any new activity in *#{table_info[:clean_table]}* table."
    return
  end
  dallas_shorts = {
      'Dal' => 'Dallas',
      'ART' => 'Art',
      'Gis' => 'GIS',
      'Dctv' => 'DCTV',
      'Ord.' => 'Ordinance',
      'Equip' => 'Equipment',
      'Ebs' => 'EBS',
      'Env' => 'Environmental',
      'Reimb' => 'Reimbursement',
      'Emer' => 'Emergency',
      'Acq' => 'Acquisition',
      'Emp' => 'Employee',
      'Hr' => 'HR',
      'Contrl' => 'Control',
      'Cis' => 'CIS',
      'Cip' => 'CIP',
      'Pbw' => 'PBW',
      'Cct' => 'CCT',
      'Tif' => 'TIF',
      'Wrr' => 'WRR',
      'Distric' => 'District',
  }
  dallas_regex = /\b#{dallas_shorts.keys.join('\b|\b')}\b/
  activities_list.each do |row|
    clean_activity = row
    # puts "#{clean_job_title[table_info[:raw_column]]}".cyan
    clean_activity[table_info[:clean_column]] = MiniLokiC::Formatize::Cleaner.org_clean(row[table_info[:raw_column]]).gsub(/(?!^)\b(#{DOWNCASE.join('|')})\b/i){|e| e.to_s.downcase }.gsub(/[-\/][a-z]/, &:upcase).gsub(dallas_regex, dallas_shorts).gsub(/O ?& ?M/, 'Operation and Maintenance')
    # puts JSON.pretty_generate(clean_job_title).yellow
    clean_activity[table_info[:clean_column]] = clean_activity[table_info[:clean_column]].split(/\b/).map{|i| i == i.upcase ? i : i.downcase}.join
    puts clean_activity[table_info[:raw_column]].yellow
    puts " " * 5 + clean_activity[table_info[:clean_column]].green
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_activity[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_activity[table_info[:raw_column]])}';
    SQL
    # puts update_query.yellow
    route.query(update_query)
  end
end

def check_fund_types(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT s.#{table_info[:raw_column]}
    FROM #{table_info[:raw_table]} s
        LEFT JOIN #{table_info[:clean_table]} c ON c.#{table_info[:raw_column]}=s.#{table_info[:raw_column]}
    WHERE c.#{table_info[:raw_column]} IS NULL;
  SQL
  fund_types_to_clean = route.query(query).to_a
  if fund_types_to_clean.empty?
    message_to_slack "There is no any new fund type in *#{table_info[:raw_table]}* table."
    return
  end
  if fund_types_to_clean.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in *#{table_info[:raw_table]}* tables."
    return
  else
    parts = fund_types_to_clean.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]})
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?
        insert_query << "('#{escape(item[table_info[:raw_column]])}'),"
      end
      insert_query = insert_query.chop + ';'
      puts insert_query.red
      route.query(insert_query)
    end
  end
  message_to_slack "#{fund_types_to_clean.count} new fund type(s) added to *db01.usa_raw.#{table_info[:clean_table]}* table. Check and clean them manually."
end