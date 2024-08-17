# Creator:      Sergii Butrymenko
# Dataset Name: Minnesota Business Licenses
# Task #:       59
# Created:      January 2022

# ruby mlc.rb --tool="clean::mn::mn_business_licenses"
# ruby mlc.rb --tool="clean::mn::mn_business_licenses" --mode='name'
# ruby mlc.rb --tool="clean::mn::mn_business_licenses" --mode='city'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    name: {
      raw_table: 'minnesota_business_licenses',
      clean_table: 'minnesota_business_licenses__names_clean',
      raw_column: 'name',
      clean_column: 'name_clean',
    },
    city: {
      raw_table: 'minnesota_business_licenses',
      clean_table: 'minnesota_business_licenses__cities_clean',
      state_column: 'registered_office_state',
      raw_column: 'registered_office_city',
      clean_column: 'registered_office_city_clean',
      # local_connection: true
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_org_names(table_info, route)
  when :city
    recent_date = get_recent_date(table_info, route)
    fill_city_table(table_info, recent_date, where_part, route)
    clean_cities(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
    # crime_types_cleaning(table_description[:description], where_part, route)
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def fix_unicode_symbols(str)
  replacement_hash = { "\u00a0" => ' ',
                       '’' => "'",
                       '‘' => "'",
                       '“' => '"',
                       '”' => '"',
                       '—' => '-',
                       '–' => '-',

  }
  s = str.dup
  replacement_hash.each { |k, v| s.gsub!(k, v) }
  s
end

def replace_unicode_symbols(str)
  replacement_hash = { "\u00a0" => ' ',
                       '’' => "'",
                       '‘' => "'",
                       '“' => '"',
                       '”' => '"',
                       '—' => '-',
                       '–' => '-',

  }
  s = str.dup
  replacement_hash.each { |k, v| s.gsub!(k, v) }
  s
end

def message_to_slack(message, type= '')
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
      state = "#{table_info[:state_column]} VARCHAR(2),"
      usa_adcp_matching_id = "usa_adcp_matching_id BIGINT(20) DEFAULT NULL,"
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
         CHARACTER SET utf8 COLLATE utf8_general_ci;
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
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || names_list.first.nil? || (names_list.count == 1 && names_list.first[table_info[:raw_column]].nil?)
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
        insert_query << "('#{escape(item[table_info[:raw_column]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_org_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  begin
    names_list.each do |row|
      clean_name = row
      prepared_name = clean_name[table_info[:raw_column]].dup.gsub('’', "'")
                        .sub(/professional limited liability company/i, 'PLLC')
                        .sub(/limited liability company/i, 'LLC')
                        .sub(/P\.\s?L\.\s?L\.\s?C\./, 'PLLC')
                        .sub(/L\.\s?L\.\s?C\./, 'LLC')
                        .sub('LLC.', 'LLC')
      if prepared_name.match?(/[^\x00-\x7F]{2}/)
        prepared_name = prepared_name.encode("Windows-1252").force_encoding("UTF-8")
        message_to_slack("Compare this name >> #{clean_name[table_info[:raw_column]]} << with >> #{prepared_name} >> name.encode(\"Windows-1252\").force_encoding(\"UTF-8\")", :warning)
      end
      # puts prepared_name
      prepared_name = prepared_name.gsub(/[“”]/, '"').gsub('’', "'").sub(/P\.\s?L\.\s?L\.\s?C\./, 'PLLC').sub(/L\.\s?L\.\s?C\./, 'LLC')
      clean_name[:clean_column] = MiniLokiC::Formatize::Cleaner.org_clean(prepared_name).sub(/ PLLC, PLLC\.?/, ', PLLC').sub(/ LLC, LLC\.?/, ', LLC')
                                    # .sub(/professional limited liability company/i, 'PLLC')
                                    # .sub(/limited liability company/i, 'LLC')
                                    # .sub(/P\.\s?L\.\s?L\.\s?C\./, 'PLLC')
                                    # .sub(/L\.\s?L\.\s?C\./, 'LLC')

      # puts JSON.pretty_generate(clean_name).yellow
      update_query = <<~SQL
        UPDATE #{table_info[:clean_table]}
        SET #{table_info[:clean_column]}='#{escape(clean_name[:clean_column])}'
        WHERE #{table_info[:clean_column]} IS NULL
          AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
      SQL
      # puts update_query.red
      route.query(update_query)
    end
  rescue Encoding::UndefinedConversionError => exception
    puts "Error during processing: #{$!}"
    puts "Backtrace:\n\t#{exception.backtrace.join("\n\t")}"
    message_to_slack("Error during processing: #{$!}\n\n```#{exception.backtrace.join("\n")}```", :error)
  end
end

def fill_city_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, r.#{table_info[:state_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
        AND r.#{table_info[:state_column]} = cl.#{table_info[:state_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL AND cl.#{table_info[:state_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL AND r.#{table_info[:raw_column]}<>''
      AND r.#{table_info[:state_column]} IS NOT NULL AND r.#{table_info[:state_column]}<>''
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]}, r.#{table_info[:state_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack ":information_source: No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, #{table_info[:state_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        insert_query << "('#{escape(item[table_info[:raw_column]])}','#{escape(item[table_info[:state_column]])}','#{item['scrape_date']}'),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_cities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}, #{table_info[:state_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  city_list = route.query(query).to_a
  return nil if city_list.empty?

  city_list.each do |item|
    # puts JSON.pretty_generate(item).green
    city_data = item.dup
    # city_data[table_info[:clean_column]] = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    city_name = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, city_data[table_info[:state_column]], 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city_name = correct_city_name
    end
    # puts city_name.black.on_red

    city_name, usa_adcp_matching_id = get_usa_adcp_matching_id(city_data[table_info[:state_column]], city_name, route)
    # puts "#{city_data[table_info[:raw_column]]} -- #{city_data[table_info[:clean_column]]} -- #{usa_adcp_matching_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(city_name)}',
        usa_adcp_matching_id = #{usa_adcp_matching_id.nil? ? "NULL" : "#{usa_adcp_matching_id}"}
      WHERE #{table_info[:raw_column]} = '#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:state_column]}='#{item[table_info[:state_column]]}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    # puts query.red
    route.query(query)
  end
end

def get_usa_adcp_matching_id(state_code, city, route)
  query = <<~SQL
    SELECT id, short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    [city, 'NULL']
  else
    [res.first['short_name'], res.first['id']]
  end
end

# def get_state_list(route)
#   query = <<~SQL
#     SELECT name AS state
#     FROM hle_resources_readonly_sync.usa_administrative_division_states;
#   SQL
#   route.query(query).to_a.map{|i| i['state']}
# end

