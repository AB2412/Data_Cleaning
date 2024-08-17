# Creator:      Sergii Butrymenko
# Dataset Name: Alaska Business Licenses
# Task #:       7
# Migrated:     January 2022

# ruby mlc.rb --tool="clean::ak::ak_business_license"
# ruby mlc.rb --tool="clean::ak::ak_business_license" --mode='holder'
# ruby mlc.rb --tool="clean::ak::ak_business_license" --mode='city'
# ruby mlc.rb --tool="clean::ak::ak_business_license" --mode='address'

SOURCE_TABLE = 'alaska_business_licenses'.freeze
RAW_CITY_STATE_ZIP = 'physical_city_state_zip'.freeze
RAW_CITY_COL = 'physical_city'.freeze
RAW_STATE_COL = 'physical_state'.freeze
RAW_ZIP_COL = 'physical_zip'.freeze
RAW_ADDRESS = 'physical_address'.freeze
CLEAN_ADDRESS = 'address_clean'.freeze
CLEAN_CITY_COL = 'city_clean'.freeze
ORG_ID_COL = 'city_org_id'.freeze

CLEAN_COLUMNS = [
  {
    name: CLEAN_ADDRESS,
    after: RAW_ADDRESS,
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: RAW_CITY_COL,
    after: RAW_CITY_STATE_ZIP,
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: RAW_STATE_COL,
    after: RAW_CITY_COL,
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: RAW_ZIP_COL,
    after: RAW_STATE_COL,
    description: 'VARCHAR(5) DEFAULT NULL',
  },
  {
    name: CLEAN_CITY_COL,
    after: RAW_ZIP_COL,
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: ORG_ID_COL,
    after: CLEAN_CITY_COL,
    description: 'VARCHAR(25) DEFAULT NULL',
  },
].freeze

def execute(options = {})
  route = C::Mysql.on(DB13, 'usa_raw')
  table_description = {
    holder: {
      raw_table: 'alaska_business_licenses',
      clean_table: 'alaska_business_licenses__names_clean',
      raw_column: 'business_name',
      clean_column: 'business_name_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :holder
    # contributors_cleaning(table_info, where_part, route)

    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_org_names(table_info, route)
  when :city
    check_clean_column_exist(CLEAN_COLUMNS, route)
    split_city_state_zip(route)
    states_list = get_states_list(route)
    if states_list.empty?
      message_to_slack('There is no any new city names to clean')
      return
    end
    states_list.each do |st|
      # puts st
      # puts
      cities_to_clean = get_cities_to_clean(st[RAW_STATE_COL], route)
      cities_to_clean.each do |row|
        clean_name = row
        clean_name[CLEAN_CITY_COL] = MiniLokiC::DataMatching::NearestWord.correct_city_name(row[RAW_CITY_COL].sub(/\bMt\.? /i, 'Mount ').sub(/\bFt\.? /i, 'Fort ').sub(/\bSt /i, 'St. ').sub(/\bSaint /i, 'St. ').sub(/^E\.? /i, 'East ').sub(/^W\.? /i, 'West ').sub(/^N\.? /i, 'North ').sub(/^S\.? /i, 'South '), st[RAW_STATE_COL], 1)
        next if clean_name[CLEAN_CITY_COL].nil? || clean_name[CLEAN_CITY_COL].empty?

        clean_name[ORG_ID_COL] = get_city_org_id(st[RAW_STATE_COL], clean_name[CLEAN_CITY_COL], route)

        # puts "#{clean_name[RAW_CITY_COL]} -- #{clean_name[CLEAN_CITY_COL]} -- #{clean_name[ORG_ID_COL]}".yellow
        update_cities(st[RAW_STATE_COL], clean_name, route)
      end
    end
  when :address
    clean_address(route)
  else
    puts 'EMPTY'.black.on_yellow
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
    text: "*[CLEANING #7] Alaska Business Licenses* \n>#{type} #{message}",
    as_user: true
  )
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    # puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
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
         name_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
    # puts create_table.red
    route.query(create_table)
    # puts 'Table created'
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
  # puts query.green
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
        insert_query << "('#{escape(item[table_info[:raw_column]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  str = str.to_s
  return if str == ''

  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def clean_org_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  # puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table."
    return
  end
  names_to_clean.each do |row|
    clean_name = row
    clean_name['skip_it'] = 0
    result_name = row[table_info[:raw_column]].strip
    if result_name.start_with?('"') && result_name.end_with?('"')
      result_name = result_name[1..-1]
    end
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
    # result_name = result_name.sub('"', '') if result_name.count('"') == 1
    result_name = result_name.sub('", "', ', ')  if result_name.count('"') == 2

    clean_name[table_info[:clean_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').gsub('. , ', '., ').gsub(', , ', ', ')
    clean_name['skip_it'] = 1 unless clean_name[table_info[:clean_column]].match?(/[a-z]/i)

    # puts JSON.pretty_generate(clean_name).yellow

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

def check_clean_column_exist(column_list, route)
  column_list.each do |item|
    query = <<~SQL
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA='usa_raw'
        AND TABLE_NAME='#{SOURCE_TABLE}'
        AND COLUMN_NAME='#{item[:name]}';
    SQL
    # puts query.green
    next unless route.query(query).to_a.empty?

    query = <<~SQL
      ALTER TABLE #{SOURCE_TABLE}
      ADD COLUMN #{item[:name]} #{item[:description]} AFTER #{item[:after]};
    SQL
    puts "Adding column #{item[:name]} to the source table #{SOURCE_TABLE}"
    route.query(query)
  end
end

def split_city_state_zip(route)
  query = <<~SQL
    SELECT DISTINCT #{RAW_CITY_STATE_ZIP}
    FROM #{SOURCE_TABLE}
    WHERE #{RAW_CITY_COL} IS NULL
      AND #{RAW_CITY_STATE_ZIP} IS NOT NULL
    ORDER BY #{RAW_CITY_STATE_ZIP};
  SQL
  # puts query
  city_state_zip_list = route.query(query).to_a
  return nil if city_state_zip_list.empty?

  city_state_zip_list.each do |item|
    # puts item
    m = item[RAW_CITY_STATE_ZIP].match(/(.+?),+\s([a-zA-Z\s]+)\s*(\d{,5})?/)
    next if m.nil?

    # puts "#{m[0]} ~~~ #{m[1]} ~~~ #{m[2]} ~~~ #{m[3]}"
    query = <<~SQL
      UPDATE #{SOURCE_TABLE}
      SET #{RAW_CITY_COL} = '#{escape(remove_not_alpha(m[1]))}',
        #{RAW_STATE_COL} = '#{remove_not_alpha(m[2])}',
        #{RAW_ZIP_COL} = '#{remove_not_alpha(m[3])}'
      WHERE #{RAW_CITY_STATE_ZIP} = '#{escape(item[RAW_CITY_STATE_ZIP])}'
        AND #{RAW_CITY_COL} IS NULL
        AND #{RAW_STATE_COL} IS NULL;
    SQL
    # puts query.green
    route.query(query).to_a
  end
end

def get_states_list(route)
  query = <<~SQL
    SELECT DISTINCT #{RAW_STATE_COL}
    FROM #{SOURCE_TABLE}
    WHERE #{CLEAN_CITY_COL} IS NULL
      AND #{RAW_STATE_COL} IS NOT NULL
      AND #{RAW_STATE_COL}<>'';
  SQL
  # puts query.yellow
  route.query(query).to_a
end

def get_cities_to_clean(state, route)
  query = <<~SQL
    SELECT DISTINCT #{RAW_CITY_COL}
    FROM #{SOURCE_TABLE}
    WHERE #{CLEAN_CITY_COL} IS NULL
      AND #{RAW_STATE_COL}='#{state}'
    ORDER BY #{RAW_CITY_COL};
  SQL
  # puts query.magenta
  route.query(query).to_a
end

def update_cities(state, city_data, route)
  # zip_list = city_data['zip_code'] ? "AND (complete_address LIKE '%" + city_data['zip_code'].join("%' OR complete_address LIKE '%") + "%')" : nil
  #{zip_list}
  query = <<~SQL
    UPDATE #{SOURCE_TABLE}
    SET #{CLEAN_CITY_COL} = '#{escape(city_data[CLEAN_CITY_COL])}',
      #{ORG_ID_COL} = '#{city_data[ORG_ID_COL]}'
    WHERE #{RAW_CITY_COL} = '#{escape(city_data[RAW_CITY_COL])}'
      AND #{CLEAN_CITY_COL} IS NULL
      AND #{RAW_STATE_COL}='#{state}';
  SQL
  # puts query.green
  route.query(query).to_a
end

def get_city_org_id(state_code, city, route, kind=nil)
  query = <<~SQL
    SELECT pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                      FROM hle_resources_readonly_sync.usa_administrative_division_states
                      WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      #{"AND kind='#{kind}'" if kind}
      AND pl_production_org_id IS NOT NULL
      AND short_name NOT IN (
            SELECT short_name
            FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
            WHERE state_name=(SELECT name
                              FROM hle_resources_readonly_sync.usa_administrative_division_states
                              WHERE short_name='#{state_code}')
              AND bad_matching IS NULL
              #{"AND kind='#{kind}'" if kind}
            GROUP BY short_name
            HAVING count(short_name) > 1);
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    nil
  else
    res.first['pl_production_org_id']
  end
end

def remove_not_alpha(str)
  str.nil? ? str : str.gsub(/\A[\.,\-'"`\s]+|[\.,\-'"`\s]+\z/, '')
end

def clean_address(route)
  query = <<~SQL
    SELECT DISTINCT #{RAW_ADDRESS}
    FROM #{SOURCE_TABLE}
    WHERE #{CLEAN_ADDRESS} IS NULL
      AND #{RAW_ADDRESS} IS NOT NULL
    ORDER BY #{RAW_ADDRESS};
  SQL

  address_list = route.query(query).to_a
  address_list.each do |item|
    up_query = <<~SQL
      UPDATE #{SOURCE_TABLE}
      SET #{CLEAN_ADDRESS}='#{escape(MiniLokiC::Formatize::Address.abbreviated_streets(item[RAW_ADDRESS]))}'
      WHERE #{RAW_ADDRESS}='#{escape(item[RAW_ADDRESS])}';
    SQL
    # puts up_query.cyan
    route.query(up_query).to_a
  end
end