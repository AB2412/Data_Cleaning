# Creator:      Sergii Butrymenko
# Dataset Name: Illinois Report Card (PARCC Schools)
# Task #:       69
# Migrated:     July 2022

# ruby mlc.rb --tool="clean::il::il_report_card_districts" --mode='street_address' --org='district'
# ruby mlc.rb --tool="clean::il::il_report_card_districts" --mode='county' --org='district'
# ruby mlc.rb --tool="clean::il::il_report_card_districts" --mode='city' --org='district'
# ruby mlc.rb --tool="clean::il::il_report_card_districts" --mode='city_all' --org='district'

def execute(options = {})
  route_db01 = C::Mysql.on(DB01, 'il_raw')

  table = case options['org']&.to_sym
          when :district
            'il_report_card_districts__clean'
          when :school
            'il_report_card_schools__clean'
          else
            nil
          end

  if table.nil?
    message_to_slack('*org* option should be specified (school/district)', :warning)
  else
    mode = options['mode']&.to_sym
    case mode
    when :street_address
      clean_address(table, route_db01)
    when :county
      clean_county(table, route_db01)
      check_county(table, route_db01)
    when :city
      clean_city(table, route_db01)
      check_city(table, route_db01)
    when :city_all
      clean_city_all(table, route_db01)
    # when :status
    #   status_checking(table_info, where_part, route)
    else
      puts 'EMPTY'.black.on_yellow
    end
  end
  route_db01.close
end

def clean_county(table, route)
  update_query = <<~SQL
    UPDATE #{table} c
        LEFT JOIN hle_resources_readonly_sync.usa_administrative_division_counties cty
        ON c.county = cty.short_name AND cty.state_id = 14
    SET c.county_clean = cty.short_name
    WHERE county_clean IS NULL
      AND county IS NOT NULL
      AND county <> ''
      AND disabled=0;
  SQL
  puts update_query
  route.query(update_query)
end

def check_county(table, route)
  query = <<~SQL
    SELECT COUNT(*) AS county_count
    FROM #{table}
    WHERE county_clean IS NULL
      AND county IS NOT NULL
      AND county <> ''
      AND disabled=0;
  SQL
  county_count = route.query(query, symbolize_keys: true).to_a.first[:county_count]
  unless county_count.nil?
    message_to_slack("#{county_count} counties at *#{table}* were not cleaned by script")
  end
end

def clean_city(table, route)
  # AND m.county_name LIKE CONCAT(c.county_clean, '%')
  update_query = <<~SQL
    UPDATE #{table} c
    LEFT JOIN hle_resources_readonly_sync.usa_administrative_division_counties_places_matching m
      ON c.city=m.short_name AND c.state=m.state_name
    SET city_clean = m.short_name
    WHERE city_clean IS NULL
      AND city IS NOT NULL
      AND city <> ''
      AND disabled=0;
  SQL
  puts update_query
  route.query(update_query)
end

def check_city(table, route)
  query = <<~SQL
    SELECT COUNT(*) AS city_count
    FROM #{table}
    WHERE city_clean IS NULL
      AND city IS NOT NULL
      AND city <> ''
      AND disabled=0;
  SQL
  city_count = route.query(query, symbolize_keys: true).to_a.first[:city_count]
  unless city_count.nil?
    message_to_slack("#{city_count} cities at *#{table}* were not cleaned by script")
  end
end

def clean_city_all(table, route)
  query = <<~SQL
    SELECT DISTINCT city
    FROM #{table}
    WHERE city_clean IS NULL
      AND city IS NOT NULL
      AND city <> ''
      AND disabled=0;
  SQL
  city_list = route.query(query, symbolize_keys: true).to_a
  city_list.each do |item|
    update_query = <<~SQL
      UPDATE #{table}
      SET city_clean='#{escape(item[:city].split(/\b/).map(&:capitalize).join)}'
      WHERE city_clean IS NULL
        AND city = '#{escape(item[:city])}'
        AND disabled=0;
    SQL
    # puts update_query
    route.query(update_query)
  end
end

def clean_address(table, route)
  query = <<~SQL
    SELECT DISTINCT street_address
    FROM #{table}
    WHERE street_address_clean IS NULL
      AND street_address IS NOT NULL
      AND street_address <> ''
      AND disabled=0;
  SQL
  address_list = route.query(query, symbolize_keys: true)
  address_list.each do |item|
    update_query = <<~SQL
      UPDATE #{table}
      SET street_address_clean = '#{escape(MiniLokiC::Formatize::Address.abbreviated_streets(item[:street_address]))}'
      WHERE street_address_clean IS NULL
        AND street_address = '#{escape(item[:street_address])}'
        AND disabled=0;
    SQL
    # puts update_query
    route.query(update_query)
  end
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
    text: "*[CLEANING #69] Illinois Report Card (PARCC Schools)* \n>#{type} #{message}",
    as_user: true
  )
end
