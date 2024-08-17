# Creator:      Sergii Butrymenko
# Dataset Name: Florida - State CC Data Cleaning
# Task #:       56
# Migrated:     November 2021

# ruby mlc.rb --tool="clean::fl::fl_state_campaign_finance"
# ruby mlc.rb --tool="clean::fl::fl_state_campaign_finance" --mode='committee'
# ruby mlc.rb --tool="clean::fl::fl_state_campaign_finance" --mode='contributor'
# ruby mlc.rb --tool="clean::fl::fl_state_campaign_finance" --mode='city'


require_relative '../../../../lib/mini_loki_c/common'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    committee: {
      raw_table: 'fl_campaign_detailed_committee',
      clean_table: 'fl_campaign_committees__uniq',
      raw_column_st: 'committee_name',
      raw_column_ct: 'trim_name',
      clean_column: 'full_clean',
    },
    contributor: {
      raw_table: 'fl_campaign_contributors',
      clean_table: 'fl_campaign_contributors__clean_names',
      raw_column: 'name',
      clean_column: 'name_cleaned',
      type_column: 'name_type',
    },
    city: {
      clean_table: 'fl_campaign_detailed_committee',
      raw_column: 'contact_city',
      clean_column: 'contact_city_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :committee
    uniq_committee(table_info, route)
    # recent_date = get_recent_date(table_info, route)
    # fill_table(table_info, recent_date, where_part, route)
    # clean_person_names(table_info, route)
  when :contributor
    contributors_cleaning(table_info, where_part, route)

    # recent_date = get_recent_date(table_info, route)
    # fill_table(table_info, recent_date, where_part, route)
    # clean_person_names(table_info, route)
  when :city
    # fill_city_table(table_info, recent_date, where_part, route)
    clean_cities_in_place(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
    # crime_types_cleaning(table_description[:description], where_part, route)
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
    text: "*[CLEANING #56] Florida - State CC Data Cleaning* \n>#{type} #{message}",
    as_user: true
  )
end

# Committee Cleaning

def uniq_committee(table_info, route)
  new_committees = get_new_committees(route)
  if new_committees.empty?
    message_to_slack('There is no any new committee name to clean')
  else
    new_committees.each do |committee|
      if trim_name_in_table(committee['trim_name'], route)
        query = <<~SQL
          UPDATE #{table_info[:clean_table]}
          SET use_it=0
          WHERE trim_name = '#{escape(committee['trim_name'])}';
        SQL
        route.query(query)
        committee['use_it'] = 0
        message = <<~MESSAGE
          Duplicate committee trim name found: "#{committee['trim_name']}". \
          All duplicate records *use_it* fields were set to 0. Please check \
          here: https://dos.elections.myflorida.com/campaign-finance/contributions/
        MESSAGE
        message_to_slack(message, :warning)
      else
        committee['use_it'] = 1
      end
      puts JSON.pretty_generate(committee).yellow
      insert(route, table_info[:clean_table], committee, true)
    end
  end
end

def get_new_committees(route)
  query = <<~SQL
    SELECT DISTINCT *
    FROM (
          SELECT cmte.committee_name AS full_name
          FROM fl_campaign_detailed_committee cmte
            LEFT JOIN fl_campaign_committees__uniq uniq ON uniq.full_name=cmte.committee_name
          WHERE full_name IS NULL) t1
      JOIN (
          SELECT DISTINCT committee_name AS trim_name
          FROM fl_campaign_contributions) t2 ON t1.full_name LIKE CONCAT(t2.trim_name, '%');
  SQL
  puts query.green
  route.query(query).to_a
end

def trim_name_in_table(trim_name, route)
  query = <<~SQL
    SELECT *
    FROM fl_campaign_committees__uniq uniq
    WHERE trim_name='#{escape(trim_name)}';
  SQL
  puts query.green
  route.query(query).to_a.empty? ? false : true
end

def insert(db, tab, h, ignore = false, log=false)
  query = "insert #{ignore ? 'ignore ' : ''}into #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')}) values (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')})"
  p query if log
  db.query(query)
end

# Contributor Cleaning

def contributors_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_mixed_names(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
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
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(50),"
      usa_adcp_matching_id = "usa_adcp_matching_id INT DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
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
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in the source table"
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

def clean_mixed_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table."
    return
  end
  det = MiniLokiC::Formatize::Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    clean_name['skip_it'] = 0
    puts clean_name[table_info[:raw_column]].to_s.cyan
    result_name = row[table_info[:raw_column]].dup.strip
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end

    if result_name.match?(/\b\d+.*(x|@|at|member|people|person).*\d+\b/i)
      clean_name['skip_it'] = 1
      clean_name[table_info[:type_column]] = 'Group'
      # result_name = result_name
    else
      clean_name[table_info[:type_column]] = det.determine(result_name)
      result_name = clean_name[table_info[:type_column]] == 'Person' ? MiniLokiC::Formatize::Cleaner.person_clean(result_name) : MiniLokiC::Formatize::Cleaner.org_clean(result_name)
    end
    case result_name.count('"')
    when 1
      result_name.sub!('"', '')
    when 2
      result_name.sub!('", "', ', ')
    end

    clean_name[table_info[:clean_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC').sub('Â¿', "'")
    # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_column]].match?(/[a-z]/i)
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}',
          #{table_info[:type_column]}='#{clean_name[table_info[:type_column]]}',
          skip_it=#{clean_name['skip_it']}
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
    # insert(route, table_info[:clean_table], clean_name, true, true)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

# def check_clean_column_exist(route)
#   query = <<~SQL
#     SELECT COLUMN_NAME
#     FROM INFORMATION_SCHEMA.COLUMNS
#     WHERE TABLE_SCHEMA='usa_raw'
#       AND TABLE_NAME='fl_campaign_detailed_committee'
#       AND COLUMN_NAME IN ('contact_city_clean', 'usa_adcp_matching_id');
#   SQL
#   if route.client.query(query).to_a.empty?
#     query = <<~SQL
#       ALTER TABLE fl_campaign_detailed_committee
#       ADD COLUMN contact_city_clean VARCHAR(255) DEFAULT NULL AFTER contact_city,
#       ADD COLUMN usa_adcp_matching_id INT DEFAULT NULL AFTER contact_city_clean;
#     SQL
#     route.client.query(query)
#   end
# end

def clean_cities_in_place(table_info, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE contact_state='FL'
      AND #{table_info[:clean_column]} IS NULL;
  SQL
  city_list = route.query(query).to_a
  return nil if city_list.empty?

  city_list.each do |item|
    puts JSON.pretty_generate(item).green
    city_name = item[table_info[:raw_column]].dup.sub(/^(PT CHARLOTTE|Charlotte)$/i, 'Port Charlotte').split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, 'FL', 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city_name = correct_city_name
    end
    puts city_name.black.on_red
    next if city_name == item[table_info[:raw_column]]

    # city_name, usa_adcp_matching_id = get_usa_adcp_matching_id('FL', city_name, route)
    # puts "#{item[table_info[:raw_column]]} -- #{city_name} -- #{usa_adcp_matching_id}".yellow
    #     usa_adcp_matching_id = #{usa_adcp_matching_id.nil? ? "NULL" : "#{usa_adcp_matching_id}"}
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(city_name)}'
      WHERE #{table_info[:raw_column]} = '#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL
        AND contact_state='FL';
    SQL
    puts query.red
    route.query(query)
  end
end

# def get_usa_adcp_matching_id(state_code, city, route)
#   query = <<~SQL
#     SELECT id, short_name, pl_production_org_id
#     FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
#     WHERE state_name=(SELECT name
#                     FROM hle_resources_readonly_sync.usa_administrative_division_states
#                     WHERE short_name='#{state_code}')
#       AND short_name='#{escape(city)}'
#       AND bad_matching IS NULL
#       AND has_duplicate=0
#       AND pl_production_org_id IS NOT NULL;
#   SQL
#   # puts query.green
#   res = route.query(query).to_a
#   if res.empty? || res.count > 1
#     [city, 'NULL']
#   else
#     [res.first['short_name'], res.first['id']]
#   end
# end

# def clean_cities_in_place(table_info, route)
#   bad_city_names = {'Ft. Lauderdale' => 'Fort Lauderdale',
#                     'PT CHARLOTTE' => 'Port Charlotte',
#                     'Charlotte' => 'Port Charlotte',
#                     "Land O' Lakes" => "Land O'Lakes",
#                     'Port Saint Lucie' => 'Port St. Lucie',
#                     'Saint Petersburg' => 'St. Petersburg',
#                     'St Petersburg' => 'St. Petersburg',
#                     'Saint Cloud' => 'St. Cloud',
#                     'Tallahasee' => 'Tallahassee',
#                     'Tallahssee' => 'Tallahassee',
#                     'TALLAHASSEE' => 'Tallahassee',
#   }
#
#   check_clean_column_exist(route)
#
#   bad_city_names.each do |raw_name, clean_name|
#     query = <<~SQL
#       UPDATE fl_campaign_detailed_committee
#       SET contact_city_clean='#{escape(clean_name)}'
#       WHERE contact_city='#{escape(raw_name)}'
#         AND contact_state='FL'
#         AND contact_city_clean IS NULL;
#     SQL
#     # puts query.green + "\n"
#     route.query(query)
#   end
# end