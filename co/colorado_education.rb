# Creator:      Sergii Butrymenko
# Dataset Name: US Schools: Colorado education
# Task #:       106
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/662
# Dataset Link: https://lokic.locallabs.com/data_sets/554
# Created:      May 2023

# ruby mlc.rb --tool="clean::co::colorado_education" --mode='schools'
# ruby mlc.rb --tool="clean::co::colorado_education" --mode='districts'
# ruby mlc.rb --tool="clean::co::colorado_education" --mode='check_unmatched'

def execute(options = {})
  route = C::Mysql.on(DB01, 'us_schools_raw')
  mode = options['mode']&.to_sym
  case mode
  when :schools
    clean_school_names(route)
  when :districts
    clean_district_names(route)
  # when :check_unmatched
  #   check_unmatched(route)
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
    text: "*[CLEANING #106] US Schools: Colorado education* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

# Schools Cleaning

def clean_school_names(route)
  query = <<~SQL
    SELECT id, name
    FROM co_general_info
    WHERE is_district=0
      AND name_clean IS NULL;
  SQL
  puts query.green
  school_names = route.query(query, symbolize_keys: true).to_a

  school_names.each do |item|
    puts JSON.pretty_generate(item).yellow
    # clean_school_name = item[:org_name].dup.sub(/\bRHS\b/i, 'Regional High School')
    #                       .sub(/\bMid\b/i, 'Middle')
    #                       .sub(/\bEd\b/i, 'Education')
    #                       .gsub(/\b(Jr|Sr)\b\.?/i){|i| i[0].downcase == 'j' ? 'Junior' : 'Senior'}
    #                       .gsub(/(?<!\bon)(?:\s)\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}
    # clean_school_name = "#{clean_school_name} School" unless clean_school_name.match?(/\b(school|preschool|center|academy|conservatory|hospital|ELC|College|program)\b/i)
    clean_school_name = item[:name].dup.gsub(/\bsch\b/i, 'School').gsub(/(?<![-'])\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}

    puts clean_school_name
    puts "#{item[:name]} >>> #{clean_school_name}".cyan if item[:name] != clean_school_name
    insert_query = <<~SQL
      UPDATE co_general_info
      SET name_clean = '#{escape(clean_school_name)}'
      WHERE id = #{item[:id]}
        AND name='#{escape(item[:name])}'
        AND name_clean IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end

# Districts Cleaning

def clean_district_names(route)
  query = <<~SQL
    SELECT id, name
    FROM co_general_info
    WHERE is_district=1
      AND name_clean IS NULL;
  SQL
  puts query.green
  district_names = route.query(query, symbolize_keys: true).to_a

  district_names.each do |item|
    puts JSON.pretty_generate(item).yellow
    clean_district = item[:name].dup.gsub(/\bAU\b/i, 'Administrative Unit').gsub(/(?<!')\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}
    if clean_district.match?(/\bNo?\.\s[-a-z0-9]+\b/i)
      parts = clean_district.match(/(?<name>.*)(?<num>No?\.\s[-a-z0-9]+((\sJt\.?)?))\s?(?<cut>.*)?/i)
      clean_district = parts[:name].split(/(?<!')\b/).map(&:capitalize).join + parts[:num]
    else
      clean_district = clean_district.split(/(?<!')\b/).map{|item| item == 'BOCES' || item.match?(/^\d/) ? item : item.capitalize}.join
    end

    puts clean_district
    puts "#{item[:name]} >>> #{clean_district}".cyan if item[:name] != clean_district
    insert_query = <<~SQL
      UPDATE co_general_info
      SET name_clean = '#{escape(clean_district)}'
      WHERE id = #{item[:id]}
        AND name='#{escape(item[:name])}'
        AND name_clean IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end

# def check_unmatched(route)
#   query = <<~SQL
#     SELECT COUNT(*) AS total_unmatched,
#            SUM(IF(city IS NULL, 0, 1)) AS with_city,
#            SUM(IF(city IS NULL, 1, 0)) AS without_city
#     FROM us_schools_raw.co_general_info
#     WHERE id IN
#       (
#         SELECT DISTINCT general_id FROM us_schools_raw.al_accountability_indicators
#         UNION
#         SELECT DISTINCT general_id FROM us_schools_raw.al_college_career_readiness
#         UNION
#         SELECT DISTINCT general_id FROM us_schools_raw.al_enrollment
#         UNION
#         SELECT DISTINCT general_id FROM us_schools_raw.al_schools_assessment
#       )
#       AND pl_production_org_id IS NULL;
#   SQL
#   # counts = route.query(query, symbolize_keys: true).to_a.first
#   unless counts[:total_unmatched].zero?
#     message_to_slack("#{counts[:total_unmatched]} unmatched orgs found in *db01.us_schools_raw.co_general_info* table and #{counts[:with_city]} of them have address (city).", counts[:with_city].zero? ? :info : :warning)
#   end
# end
