# Creator:      Sergii Butrymenko
# Dataset Name: Illinois Sales Taxes
# Task #:       197
# Created:      January 2022

# ruby mlc.rb --tool="clean::il::sales_taxes"
# ruby mlc.rb --tool="clean::il::sales_taxes" --mode='location'

def execute(options = {})
  route = C::Mysql.on(DB01, 'us_sales_taxes')
  table_description = {
    location: {
      raw_table: 'illinois_by_location',
      clean_table: 'illinois__locations_clean',
      raw_column: 'local_government',
      clean_column: 'local_government_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  # table_info = table_description[mode]
  case mode
  when :location
    table_info = table_description[mode]
    recent_date = get_recent_date(table_info, route)
    fill_location_table(table_info, recent_date, where_part, route)
    clean_counties(table_info, route)
    clean_cities(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
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
    text: "*[CLEANING #59] Minnesota Business Licenses* \n>#{type} #{message}",
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
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    # constraints = "UNIQUE (#{table_info[:raw_column]})"
    # type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    # if table_info[:state_column]
    #   state = "#{table_info[:state_column]} VARCHAR(2),"
    #   usa_adcp_matching_id = "usa_adcp_matching_id BIGINT(20) DEFAULT NULL,"
    #   constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    # else
    #   state = nil
    #   usa_adcp_matching_id = nil
    # end
    # create_table = <<~SQL
    #   CREATE TABLE #{table_info[:clean_table]}
    #     (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
    #      #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
    #      #{table_info[:clean_column]} VARCHAR(255),
    #      #{type}
    #      #{state}
    #      #{usa_adcp_matching_id}
    #      fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #      scrape_date DATE NOT NULL DEFAULT '0000-00-00',
    #      created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    #      updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #      #{constraints})
    #      CHARACTER SET utf8 COLLATE utf8_general_ci;
    # SQL
    #{local_connection}
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         county varchar(100) DEFAULT NULL,
         is_county BOOLEAN NOT NULL DEFAULT 0,
         usa_adcp_matching_id bigint(20) DEFAULT NULL,
         usa_adc_matching_id bigint(20) DEFAULT NULL,
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_by varchar(255) DEFAULT 'Sergii Butrymenko',
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
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_location_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL AND r.#{table_info[:raw_column]}<>'' AND r.#{table_info[:raw_column]}<>'TOTAL'
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
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
        insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}'),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_counties(table_info, route)
  query = <<~SQL
    UPDATE #{table_info[:clean_table]} t
      LEFT JOIN hle_resources_readonly_sync.usa_administrative_division_counties c
        ON c.name = REPLACE(REPLACE(REPLACE(TRIM(TRAILING
          SUBSTRING_INDEX(#{table_info[:raw_column]}, 'COUNTY', -1) FROM t.#{table_info[:raw_column]}),
            'SAINT CLAIR', 'ST. CLAIR'),
            'DEWITT', 'DE WITT'),
            'LA SALLE', 'LASALLE')
    SET #{table_info[:clean_column]}=name,
        usa_adc_matching_id=c.id,
        is_county=1
    WHERE #{table_info[:clean_column]} IS NULL
      AND #{table_info[:raw_column]} RLIKE 'COUNTY'
      AND state_id = (SELECT id
                      FROM hle_resources_readonly_sync.usa_administrative_division_states
                      WHERE short_name = 'IL');
  SQL
  puts query.red
  route.query(query)
end

def clean_cities(table_info, route)
  query = <<~SQL
    UPDATE #{table_info[:clean_table]} t
      LEFT JOIN hle_resources_readonly_sync.usa_administrative_division_counties_places_matching p
        ON p.short_name = t.#{table_info[:raw_column]} AND state_name = 'Illinois' AND bad_matching IS NULL
    SET t.usa_adcp_matching_id=p.id,
        t.#{table_info[:clean_column]}=p.short_name,
        t.county=p.county_name
    WHERE t.#{table_info[:clean_column]} IS NULL;
  SQL
  puts query.red
  route.query(query)
end



# def get_usa_adcp_matching_id(state_code, city, route)
#   query = <<~SQL
#     SELECT id, short_name, pl_production_org_id
#     FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
#     WHERE state_name=(SELECT name
#                       FROM hle_resources_readonly_sync.usa_administrative_division_states
#                       WHERE short_name='#{state_code}')
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
