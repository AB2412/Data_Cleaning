# frozen_string_literal: true

# Creator: Alex Kuzmenko; Based on Loki files created by A.Kuzmenko & D.Buzina
# Update: Alberto Egurrola - January 2023
# [bundle exec] ruby mlc.rb --tool='clean::nc::nc_state_cc_cleanings' --cleaning_type=1
# cleaning types are between 1 and 6 according to the required files
require_relative 'nc_state_cc/candidates'
require_relative 'nc_state_cc/committee_cities'
require_relative 'nc_state_cc/committees'
require_relative 'nc_state_cc/contributor_cities'
require_relative 'nc_state_cc/contributors'
require_relative 'nc_state_cc/recipients'
require_relative 'nc_state_cc/expenditures'
require_relative '../../../../lib/mini_loki_c/hle_clean_common'

require 'damerau-levenshtein'

DB = MiniLokiC::HLECleanCommon::DB
TOOLS = MiniLokiC::HLECleanCommon::Tools
OPTIONS = MiniLokiC::HLECleanCommon::Options.new

def execute(options = {})
  OPTIONS.merge!(options.clone)

  if options['cleaning_type']
    case options['cleaning_type']
    when '1'
      puts 'Algorithms are unnecessary' # candidates_cleaning
    when '2'
      committee_cities_cleaning
    when '3'
      committees_cleaning
    when '4'
      contributor_cities_cleaning
    when '5'
      contributors_cleaning
    when '6'
      recipients_cleaning
    when '7'
      expenditures_cleaning
    when '8'
      expenditures_locations_cleaning
    else
      puts 'Wrong --cleaning_type, Execute cleaning with --cleaning_type = one of (1..8)'
    end
  else
    puts 'No --cleaning_type, Execute cleaning with --cleaning_type = one of (1..8)'
  end
end

# methods shared to some of files required above
ABBR_LOCAL = /\b((\w?[^aeoiuy'\s-]{2,5})|([^aeoiuy'\s-]{2}\w)|[aeoiuy]{3})\b/i.freeze
BUSINESS_SUFFIXES_LOCAL = /\b(NCSFAA|NCAE|DA|DEC|REC|LEC|DM|RM|DW|RW|GP|LTD|LIMITED|LCCC|LCCP|PAC|MD|BPC|AAO|LLL?P?|PLL?C|P\.?\s*(?:C|L)|P\s*A\s*A|L\.?(?:imited)?\s*L\.?(?:iability)?(\s*C\.?(?:o(mp)?\.?(any)?)?)?|D\.?D\.?S\.?|INC\.?O?R?P?O?R?A?T?E?D?|N\.?\s*A\.?|L\.?(?:imited)?\s*P\.?(?:artn?e?r?s?(hip)?)?|COR?P?\.?(ORATION)?|S\.?A|S\.?S\.?B|P\.?L?\.?(C\.?C?|A\.?A|L))(\.|\b)/i.freeze
CORRECTIONS_LOCAL = {
  /\bJr\b\.?/i => 'Jr.',
  /\bJR\b\.?/i => 'Jr.',
  /\bLTD\b\.?/i => 'Ltd.',
  /\bINCO?R?P?O?R?A?T?E?D?\b\.?/i => 'Inc.',
  /\bCORP(ORATION)?\b\.?/i => 'Corp.',
  /\bCOM?P?\b\.?/i => 'Co.',
  /\bLLC\b\.?/i => 'LLC',
  /\bN\.?\s*A\b\.?/i => 'N.A.',
  /(\b|\()u(\.\s?|\s)?s(\b\.?|\.?\))?a?(\.?\)|\b\.?)/i => 'USA',
  /\bbk\b/i => 'Bank',
  /\bCmnty\b/i => 'Community',
  /\bTC\b/i => 'Trust',
  /\bSVC\b/i => 'Services',
  /\b B \b/i => 'Bank',
  /\bSt\b/i => 'State',
  /\bMV\b/i => 'Midwest',
  /\bcty\b/i => 'County',
  /\bCMRC\b/i => 'Commerce',
  /\bFNCL\b/i => 'Financial',
  /\bBKG\b/i => 'Banking',
  /\bTr\b/i => 'Trust',
  /\b1st\b/i => 'First',
  /\bN. a\b/i => 'N.A.',
  /\bSVGS\b/i => 'Savings',
  /\bCMRL\b/i => 'Commercial',
  /\bMNT\b/i => 'Mountain',
  /\bMtg\b/i => 'Mortgage',
  /\bDEC\b/i => 'Democratic Party',
  /\bREC\b/i => 'Republican Party',
  /\bLEC\b/i => 'Libertarian Party',
  /\bDM\b/i => 'Democratic Men',
  /\bRM\b/i => 'Republican Men',
  /\bDW\b/i => 'Democratic Women',
  /\bRW\b/i => 'Republican Women',
  /\bCOMM\b/i => 'Committee',
  /\bASSN\b/i => 'Association',
  /\bNC\b/i => 'North Carolina',
  /\bCONG\b/i => 'Congressional',
  /\bDIST\b/i => 'District'
}.freeze
CORRECTIONS_IND_LOCAL = {
  /\bJr\b\.?/i => 'Jr.',
  /\bJR\b\.?/i => 'Jr.',
  /\bANN\b\.?/i => 'Ann'
}.freeze
CORRECTIONS_NOT_IND_LOCAL = {
  /\bLTD\b\.?/i => 'Ltd.',
  /\bINCO?R?P?O?R?A?T?E?D?\b\.?/i => 'Inc.',
  /\bCORP(ORATION)?\b\.?/i => 'Corp.',
  /\bCOM?P?\b\.?/i => 'Co.',
  /\bLLC\b\.?/i => 'LLC',
  /\bN\.?\s*A\b\.?/i => 'N.A.',
  /(\b|\()u(\.\s?|\s)?s(\b\.?|\.?\))?a?(\.?\)|\b\.?)/i => 'USA',
  /\bbk\b/i => 'Bank',
  /\bCmnty\b/i => 'Community',
  /\bTC\b/i => 'Trust',
  /\bSVC\b/i => 'Services',
  /\b B \b/i => 'Bank',
  /\bSt\b/i => 'State',
  /\bMV\b/i => 'Midwest',
  /\bcty\b/i => 'County',
  /\bCMRC\b/i => 'Commerce',
  /\bFNCL\b/i => 'Financial',
  /\bBKG\b/i => 'Banking',
  /\bTr\b/i => 'Trust',
  /\b1st\b/i => 'First',
  /\bN. a\b/i => 'N.A.',
  /\bSVGS\b/i => 'Savings',
  /\bCMRL\b/i => 'Commercial',
  /\bMNT\b/i => 'Mountain',
  /\bMtg\b/i => 'Mortgage'
}.freeze

SKIPS = {
  /https?:/i => 1,
  /<\/?[^>]+\/?>/i => 1,
  /^\s*[\d.,:;\-_\s]+\s*$/i => 1,
}

def easy_titleize(line)
  fg_dash = 8210.chr(Encoding::UTF_8).to_s
  en_dash = 8211.chr(Encoding::UTF_8).to_s
  em_dash = 8212.chr(Encoding::UTF_8).to_s
  hor_bar = 8213.chr(Encoding::UTF_8).to_s
  str = "(^| |-|#{fg_dash}|#{en_dash}|#{em_dash}|#{hor_bar}){1}([a-zA-Z]{1})"
  line.downcase.gsub(/#{str}/) { "#{Regexp.last_match(1)}#{Regexp.last_match(2).upcase}" }
end

def general_normalize(name)
  name.gsub(/ /i, ' ').sub(/[\s.,]+$/, '').gsub(/,\s?/i, ', ').gsub(/\s?&\s?/i, ' & ')
end

def mac_mc(line)
  return '' if line.to_s == ''

  line.split.map! do |word|
    word.sub(/^(MAC)([^aeiou]{1})(\w*)$/i) { "#{$1.capitalize}#{$2.upcase}#{$3}" }
    .sub(/^(MC)(\w{1})(\w*)$/i) { "#{$1.capitalize}#{$2.upcase}#{$3}" }
    .sub(/^MacK$/, 'Mack')
  end.join(' ')
end

def mega_capitalize(name)
  return if name.to_s.strip.squeeze(' ') == ''

  name = name.downcase
  name.split(' ').map! do |e|
    case e
    when /[a-z]'[a-z]{2,}/i
      irish_name = e.split("'")
      irish_name[0..1].map!(&:capitalize!)
      irish_name.join("'")
    when /"/
      e.split(/(")/).map { |j| j.split('-').map(&:capitalize).join('-') }.join
    when /\(/
      e.split(/(\()/).map { |j| j.split('-').map(&:capitalize).join('-') }.join
    when /'/
      e.split(/(')/).map { |j| j.split('-').map(&:capitalize).join('-') }.join
    else
      e.split('-').map(&:capitalize).join('-')
    end
  end.join(' ')
end

def num_prefix_normalize(str)
  str.sub(/\b(0*\d+)(st|nd|rd|th)\b/i) { "#{Regexp.last_match(1).to_i}#{Regexp.last_match(2)}" }
end

def pubs_matching_empty?(city, db_pub)
  checking_query = <<~SQL
    SELECT DISTINCT
      mat.short_name AS city
    FROM usa_administrative_division_counties_places_matching AS mat
    JOIN usa_administrative_division_states AS st
      ON st.name = mat.state_name
    WHERE st.short_name = 'NC'
      AND mat.short_name = #{city.dump};
  SQL
  db_pub.query(checking_query).to_a.empty?
end

def state_list_cities(db_pub)
  list_query = <<~SQL
    SELECT DISTINCT
      mat.short_name AS city
    FROM usa_administrative_division_counties_places_matching AS mat
    JOIN usa_administrative_division_states AS st
      ON st.name = mat.state_name
    WHERE st.short_name = 'NC';
  SQL
  db_pub.query(list_query).to_a.map { |el| el['city'] }
end

def try_fixing_city(city, list, mistakes = 3)
  dl = DamerauLevenshtein
  h = {}
  max_miss = mistakes
  list.each do |good_city|
    dist = dl.distance(city.downcase, good_city.downcase, 1, 3)
    mistakes = dist if dist < mistakes
    h[dist] = [] unless h[dist]
    h[dist] << good_city
  end

  result =
    if mistakes >= max_miss
      nil
    else
      h[mistakes].count > 1 ? nil : h[mistakes][0]
    end
  puts "fixing method was used for #{city}; #{mistakes} misses; result matched to #{result}" if result && mistakes != 1

  result
end
